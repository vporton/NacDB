import Principal "mo:base/Principal";
import BTree "mo:btree/BTree";
import RBT "mo:stable-rbtree/StableRBTree";
import Text "mo:base/Text";
import Nat "mo:base/Nat";
import Prim "mo:⛔";
import Buffer "mo:base/Buffer";
import Debug "mo:base/Debug";
import Bool "mo:base/Bool";

module {
    public type SubDBKey = Nat;

    public type SK = Text;

    public type AttributeKey = Text;

    public type AttributeValuePrimitive = {#text : Text; #int : Int; #bool : Bool; #float : Float};
    public type AttributeValueBlob = {#blob : Blob};
    public type AttributeValueTuple = {#tuple : [AttributeValuePrimitive]};
    public type AttributeValueArray = {#arrayText : [Text]; #arrayInt : [Int]; #arrayBool : [Bool]; #arrayFloat : [Float]};
    public type AttributeValueRBTreeValue = AttributeValuePrimitive or AttributeValueBlob or AttributeValueTuple or AttributeValueArray;
    public type AttributeValueRBTree = {#tree : RBT.Tree<Text, AttributeValueRBTreeValue>};
    public type AttributeValue = AttributeValuePrimitive or AttributeValueBlob or AttributeValueTuple or AttributeValueArray or AttributeValueRBTree;

    type SubDB = {
        data: RBT.Tree<SK, AttributeValue>;
        hardCap: ?Nat; // Remove looser items after reaching this count. // TODO
    };

    type MoveCap = { #numDBs: Nat; #usedMemory: Nat };

    // FIXME: Don't pass this big value as function arguments.
    type SuperDB = {
        var nextKey: Nat;
        subDBs: BTree.BTree<SubDBKey, SubDB>;
        moveCap: MoveCap;
        /// Should be idempotent.
        moveCallback: ?(shared ({oldCanister: DBCanister; oldSubDBKey: SubDBKey; newCanister: DBCanister; newSubDBKey: SubDBKey}) -> async ());
        var isMoving: Bool;
        var moving: ?{ // TODO: Can we move it to `SubDB`?
            oldCanister: DBCanister;
            oldSuperDB: SuperDB;
            oldSubDBKey: SubDBKey;
            newCanister: DBCanister;
            var stage: {#moving; #notifying : {newSubDBKey: SubDBKey}}
        };
    };

    type DBIndex = {
        canisters: Buffer.Buffer<Principal>;
    };

    type IndexCanister = actor {
        getCanisters(): async [DBCanister];
        newCanister(): async DBCanister;
    };

    type DBCanister = actor {
        isOverflowed() : async Bool;
        insertSubDB(data: RBT.Tree<SK, AttributeValue>) : async SubDBKey;
    };

    func insertSubDB(superDB: SuperDB, subDB: SubDB): SubDBKey {
        switch (superDB.moving) {
            case (?_) { Debug.trap("DB is scaling") };
            case (null) {
                let key = superDB.nextKey;
                superDB.nextKey += 1;
                ignore BTree.insert(superDB.subDBs, Nat.compare, key, subDB);
                key;
            };
        };
    };

    public func getSubDB(superDB: SuperDB, subDBKey: SubDBKey) : ?SubDB {
        BTree.get(superDB.subDBs, Nat.compare, subDBKey);
    };

    func startMoveSubDB(options: {oldCanister: DBCanister; newCanister: DBCanister; superDB: SuperDB; subDBKey: SubDBKey}) {
        switch (options.superDB.moving) {
            case (?_) { Debug.trap("already moving") };
            case (null) {
                options.superDB.moving := ?{
                    oldCanister = options.oldCanister;
                    oldSuperDB = options.superDB;
                    oldSubDBKey = options.subDBKey;
                    newCanister = options.newCanister;
                    var stage = #moving;
                }
            };
        };
    };

    func finishMoveSubDB(options: {superDB: SuperDB}) : async* () {
        switch (options.superDB.moving) {
            case (?moving) {
                switch (moving.stage) {
                    case (#moving) {
                        switch (BTree.get(moving.oldSuperDB.subDBs, Nat.compare, moving.oldSubDBKey)) {
                            case (?subDB) {
                                let newSubDBKey = await moving.newCanister.insertSubDB(subDB.data);
                                ignore BTree.delete(options.superDB.subDBs, Nat.compare, moving.oldSubDBKey);
                                moving.stage := #notifying {newCanister = moving.newCanister; newSubDBKey};
                            };
                            case (null) {};
                        };
                    };
                    case (#notifying {newSubDBKey: SubDBKey}) {
                        switch (options.superDB.moveCallback) {
                            case (?cb) {
                                await cb({
                                    oldCanister = moving.oldCanister;
                                    oldSubDBKey = moving.oldSubDBKey;
                                    newCanister = moving.newCanister;
                                    newSubDBKey: SubDBKey;
                                })
                            };
                            case (null) {};
                        };
                        options.superDB.moving := null;
                    };
                };
            };
            case (null) {};
        }
    };

    // FIXME: Race creates two new canisters.
    func doStartMovingSubDBToNewCanister(
        options: {index: IndexCanister; oldCanister: DBCanister; oldSuperDB: SuperDB; oldSubDBKey: SubDBKey}) : async* ()
    {
        let newCanister = await options.index.newCanister();
        startMoveSubDB({oldCanister = options.oldCanister; newCanister; superDB = options.oldSuperDB; subDBKey = options.oldSubDBKey});
    };

    func startMovingSubDB(options: {index: IndexCanister; oldCanister: DBCanister; oldSuperDB: SuperDB; oldSubDBKey: SubDBKey}) : async* () {
        if (options.superDB.isMoving) {
            Debug.trap("is moving");
        };
        options.superDB.isMoving := true;
        let pks = await options.index.getCanisters();
        let lastCanister = pks[pks.size()-1];
        if (lastCanister == options.oldCanister or (await lastCanister.isOverflowed())) {
            await* doStartMovingSubDBToNewCanister({
                index = options.index;
                oldCanister = options.oldCanister;
                oldSuperDB = options.superDB;
                oldSubDBKey = options.oldSubDBKey;
            });
        } else {
            startMoveSubDB({
                oldCanister = options.oldCanister;
                newCanister = lastCanister;
                superDB = options.oldSuperDB;
                subDBKey = options.oldSubDBKey;
            });
        };
        options.oldSuperDB.isMoving := false;
    };

    func startMovingSubDBIfOverflow(
        options: {indexCanister: IndexCanister; oldCanister: DBCanister; oldSuperDB: SuperDB; oldSubDBKey: SubDBKey}): async* ()
    {
        let overflow = switch (options.oldSuperDB.moveCap) {
            case (#numDBs num) {
                let ?subDB = BTree.get(options.oldSuperDB.subDBs, Nat.compare, options.oldSubDBKey) else {
                    Debug.trap("no sub DB");
                };
                RBT.size(subDB.data) > num;
            };
            case (#usedMemory mem) {
                Prim.rts_heap_size() > mem;
            };
        };
        if (overflow) {
            await* startMovingSubDB({
                index = options.indexCanister;
                oldCanister = options.oldCanister;
                oldSuperDB = options.oldSuperDB;
                oldSubDBKey = options.oldSubDBKey;
            });
        }
    };

    // DB operations //

    // FIXME: Arguments transfer the entire sub-DB!
    public type GetOptions = {subDB: SubDB; sk: SK};

    public func get(options: GetOptions) : ?AttributeValue {
        RBT.get(options.subDB.data, Text.compare, options.sk);
    };

    public type ExistsOptions = GetOptions;

    public func has(options: ExistsOptions) : Bool {
        get(options) != null;
    };
};