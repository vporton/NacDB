import Principal "mo:base/Principal";
import BTree "mo:btree/BTree";
import RBT "mo:stable-rbtree/StableRBTree";
import Text "mo:base/Text";
import Nat "mo:base/Nat";
import Prim "mo:⛔";
import Buffer "mo:base/Buffer";
import Debug "mo:base/Debug";

module {
    public type PK = Principal;

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

    // TODO: Max items per DB with auto removal of loosers.
    type SubDB = {
        // pk: PK;
        // subDBKey: SubDBKey;
        data: RBT.Tree<SK, AttributeValue>;
        var inMoving: Bool; // While moving to another canister, write operations are disabled.
        hardCap: Nat; // Remove looser items after reaching this count.
    };

    type MoveCap = { #numDBs: Nat; #usedMemory: Nat };

    type SuperDB = {
        var nextKey: Nat;
        subDBs: BTree.BTree<SubDBKey, SubDB>;
        moveCap: MoveCap;
        moveCallback: ?(shared (oldPK: PK, oldSubDBKey: SubDBKey, newPK: PK, newSubDBKey: SubDBKey) -> ());
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
        // getSuperDB() : async SuperDB;
        putSubDB(data: RBT.Tree<SK, AttributeValue>) : async ();
    };

    func putSubDB(superDB: SuperDB, subDB: SubDB) {
        let key = superDB.nextKey;
        superDB.nextKey += 1;
        ignore BTree.insert(superDB.subDBs, Nat.compare, key, subDB);
    };

    public func getSubDB(superDB: SuperDB, subDBKey: SubDBKey) : ?SubDB {
        BTree.get<SubDBKey, SubDB>(superDB.subDBs, Nat.compare, subDBKey);
    };

    public type GetOptions = {subDB: SubDB; sk: SK};

    public func get(options: GetOptions) : ?AttributeValue {
        RBT.get(options.subDB.data, Text.compare, options.sk);
    };

    public type ExistsOptions = GetOptions;

    public func has(options: ExistsOptions) : Bool {
        get(options) != null;
    };

    // FIXME: Is it idempotent?
    func doMoveSubDB(options: {newCanister: DBCanister; superDB: SuperDB; subDBKey: SubDBKey}) : async* () {
        let subDBOrNull = BTree.get(options.superDB.subDBs, Nat.compare, options.subDBKey);
        let ?subDB = subDBOrNull else { Debug.trap("no sub-DB") };
        subDB.inMoving := true;
        await options.newCanister.putSubDB(subDB.data);
        ignore BTree.delete(options.superDB.subDBs, Nat.compare, options.subDBKey);
        subDB.inMoving := false;
        // TODO: Message
    };

    func doMoveSubDBToNewCanister(options: {index: IndexCanister; superDB: SuperDB; subDBKey: SubDBKey}) : async* () {
        let newCanister = await options.index.newCanister();
        await* doMoveSubDB({newCanister; superDB = options.superDB; subDBKey = options.subDBKey});
    };

    func moveSubDB(options: {index: IndexCanister; currentCanister: DBCanister; superDB: SuperDB; subDBKey: SubDBKey}) : async* () {
        let pks = await options.index.getCanisters();
        let lastCanister = pks[pks.size()-1];
        if (lastCanister == options.currentCanister) {
            await* doMoveSubDBToNewCanister({index = options.index; superDB = options.superDB; subDBKey = options.subDBKey});
        } else if (await lastCanister.isOverflowed()) {
            await* doMoveSubDBToNewCanister({index = options.index; superDB = options.superDB; subDBKey = options.subDBKey});
        } else {
            await* doMoveSubDB({newCanister = lastCanister; superDB = options.superDB; subDBKey = options.subDBKey});
        };
    };

    // TODO: Simplify arguments.
    func moveSubDBIfOverflow(options: {indexCanister: IndexCanister; currentCanister: DBCanister; superDB: SuperDB; subDBKey: SubDBKey}): async* () {
        let overflow = switch (options.superDB.moveCap) {
            case (#numDBs num) {
                let ?subDB = BTree.get(options.superDB.subDBs, Nat.compare, options.subDBKey) else {
                    Debug.trap("no sub DB"); // FIXME: correct?
                };
                RBT.size(subDB.data) > num;
            };
            case (#usedMemory mem) {
                Prim.rts_heap_size() > mem;
            };
        };
        if (overflow) {
            await* moveSubDB({
                index = options.indexCanister;
                currentCanister = options.currentCanister;
                superDB = options.superDB;
                subDBKey = options.subDBKey;
            });
        }
    };
};