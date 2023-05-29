import I "mo:base/Iter";
import Principal "mo:base/Principal";
import BTree "mo:btree/BTree";
import RBT "mo:stable-rbtree/StableRBTree";
import StableBuffer "mo:stable-buffer/StableBuffer";
import Text "mo:base/Text";
import Nat "mo:base/Nat";
import Prim "mo:⛔";
import Buffer "mo:base/Buffer";
import Debug "mo:base/Debug";
import Bool "mo:base/Bool";
import Deque "mo:base/Deque";

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
        var data: RBT.Tree<SK, AttributeValue>;
        hardCap: ?Nat; // Remove "looser" items (with least key values) after reaching this count.
        var busy: Bool; // Forbid to move this entry to other canister. // FIXME: check on reading, too?
                        // During the move it is true. Deletion in old canister and setting it to false happen in the same atomic action,
                        // so moving is also protected by this flag.
    };

    type MoveCap = { #numDBs: Nat; #usedMemory: Nat };

    type MoveCallback = shared ({oldCanister: PartitionCanister; oldSubDBKey: SubDBKey; newCanister: PartitionCanister; newSubDBKey: SubDBKey}) -> async ();

    type CreateCallback = shared ({canister: PartitionCanister; subDBKey: SubDBKey}) -> async ();

    type CreatingSubDB = {
        canister: PartitionCanister;
        subDBKey: SubDBKey
    };

    type SuperDB = {
        var nextKey: Nat;
        subDBs: BTree.BTree<SubDBKey, SubDB>;
        moveCap: MoveCap;
        /// Should be idempotent.
        moveCallback: ?MoveCallback;
        /// Should be idempotent.
        createCallback: ?CreateCallback;
        var moving: ?{
            oldCanister: PartitionCanister;
            oldSuperDB: SuperDB;
            oldSubDBKey: SubDBKey;
            newCanister: PartitionCanister;
            var stage: {#moving; #notifying : {newSubDBKey: SubDBKey}}
        };
        var creatingSubDB: Deque.Deque<CreatingSubDB>;
    };

    // TODO: Move this and the following function below in the code:
    // It does not touch old items, so no locking.
    // TODO: Repeatedly calling this constitutes a DoS attack.
    func startCreatingSubDB({canister: PartitionCanister; superDB: SuperDB; hardCap: ?Nat}) {
        // FIXME: Deque has no `size()`.
        // if (Deque.size(superDB.creatingSubDB) >= 10) { // TODO: Make configurable.
        //     Debug.trap("queue full");
        // };
        let subDBKey = createSubDB({superDB; hardCap; busy = true});
        let ?item = Deque.peekBack(superDB.creatingSubDB) else {
            Debug.trap("programming error");
        };
        superDB.creatingSubDB := Deque.pushFront(superDB.creatingSubDB, {
            canister; subDBKey = subDBKey;
        } : CreatingSubDB);
    };

    func finishCreatingSubDB(superDB: SuperDB) : async () {
        while (not Deque.isEmpty(superDB.creatingSubDB)) {
            let ?item = Deque.peekBack(superDB.creatingSubDB) else {
                Debug.trap("no such item");
            };
            // TODO: Add also another, non-shared, callback?
            switch (superDB.createCallback) {
                case (?cb) { await cb({canister = item.canister; subDBKey = item.subDBKey}); };
                case (null) {};
            };
            let ?entry = BTree.get(superDB.subDBs, Nat.compare, item.subDBKey) else {
                Debug.trap("entry must exist");
            };
            entry.busy := false;
            switch (Deque.popBack(superDB.creatingSubDB)) { // marks as completed
                case (?(deque, _)) {
                    superDB.creatingSubDB := deque;
                };
                case (null) {
                    Debug.trap("programming error")
                };
            }
        };
    };

    public type DBIndex = {
        canisters: StableBuffer.StableBuffer<Principal>;
    };

    public type IndexCanister = actor {
        getCanisters(): async [PartitionCanister];
        newCanister(): async PartitionCanister;
    };

    public type PartitionCanister = actor {
        insertSubDB(data: RBT.Tree<SK, AttributeValue>) : async SubDBKey;
        isOverflowed() : async Bool;
    };

    public func createDBIndex() : DBIndex {
        {
            canisters = StableBuffer.init<Principal>();
        }
    };

    public func createSuperDB(options: {moveCap: MoveCap; moveCallback: ?MoveCallback; createCallback: ?CreateCallback}) : SuperDB {
        {
            var nextKey = 0;
            subDBs = BTree.init<SubDBKey, SubDB>(null);
            moveCap = options.moveCap;
            moveCallback = options.moveCallback;
            createCallback = options.createCallback;
            var moving = null;
            var creatingSubDB = Deque.empty();
        }
    };

    public func insertSubDB(superDB: SuperDB, subDB: SubDB): SubDBKey {
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

    func startMovingSpecifiedSubDB(options: {oldCanister: PartitionCanister; newCanister: PartitionCanister; superDB: SuperDB; subDBKey: SubDBKey}) {
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
        label cycle loop {
            switch (options.superDB.moving) {
                case (?moving) {
                    switch (moving.stage) {
                        case (#moving) {
                            switch (BTree.get(moving.oldSuperDB.subDBs, Nat.compare, moving.oldSubDBKey)) {
                                case (?subDB) {
                                    if (subDB.busy) {
                                        Debug.trap("entry is busy");
                                    };
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
                            let ?item = BTree.get(options.superDB.subDBs, Nat.compare, moving.oldSubDBKey) else {
                                Debug.trap("item must exist")
                            };
                            item.busy := false;
                            options.superDB.moving := null;
                        };
                    };
                };
                case (null) { break cycle; };
            }
        }
    };

    func doStartMovingSubDBToNewCanister(
        options: {index: IndexCanister; oldCanister: PartitionCanister; oldSuperDB: SuperDB; oldSubDBKey: SubDBKey}) : async* ()
    {
        let newCanister = await options.index.newCanister();
        startMovingSpecifiedSubDB({oldCanister = options.oldCanister; newCanister; superDB = options.oldSuperDB; subDBKey = options.oldSubDBKey});
    };

    func startMovingSubDB(options: {index: IndexCanister; oldCanister: PartitionCanister; oldSuperDB: SuperDB; oldSubDBKey: SubDBKey}) : async* () {
        let ?item = BTree.get(options.oldSuperDB.subDBs, Nat.compare, options.oldSubDBKey) else {
            Debug.trap("item must exist")
        };
        if (item.busy) {
            Debug.trap("is moving");
        };
        item.busy := true;
        let pks = await options.index.getCanisters();
        let lastCanister = pks[pks.size()-1];
        if (lastCanister == options.oldCanister or (await lastCanister.isOverflowed())) {
            await* doStartMovingSubDBToNewCanister({
                index = options.index;
                oldCanister = options.oldCanister;
                oldSuperDB = options.oldSuperDB;
                oldSubDBKey = options.oldSubDBKey;
            });
        } else {
            startMovingSpecifiedSubDB({
                oldCanister = options.oldCanister;
                newCanister = lastCanister;
                superDB = options.oldSuperDB;
                subDBKey = options.oldSubDBKey;
            });
        };
    };

    public func isOverflowed(superDB: SuperDB) : Bool {
        switch (superDB.moveCap) {
            case (#numDBs num) {
                BTree.size(superDB.subDBs) > num;
            };
            case (#usedMemory mem) {
                Prim.rts_heap_size() > mem;
            };
        };
    };

    func startMovingSubDBIfOverflow(
        options: {indexCanister: IndexCanister; oldCanister: PartitionCanister; oldSuperDB: SuperDB; oldSubDBKey: SubDBKey}): async* ()
    {
        if (isOverflowed(options.oldSuperDB)) {
            await* startMovingSubDB({
                index = options.indexCanister;
                oldCanister = options.oldCanister;
                oldSuperDB = options.oldSuperDB;
                oldSubDBKey = options.oldSubDBKey;
            });
        }
    };

    func trapMoving({superDB: SuperDB; subDBKey: SubDBKey}) {
        switch (BTree.get(superDB.subDBs, Nat.compare, subDBKey)) {
            case (?item) {
                if (item.busy) {
                    Debug.trap("item busy");
                }
            };
            case (null) { // TODO: needed?
                Debug.trap("item busy");
            };
        };
    };

    func removeLoosers(subDB: SubDB) {
        switch (subDB.hardCap) {
            case (?hardCap) {
                while (RBT.size(subDB.data) > hardCap) {
                    let iter = RBT.entries(subDB.data);
                    switch (iter.next()) {
                        case (?(k, v)) {
                            subDB.data := RBT.delete(subDB.data, Text.compare, k);
                        };
                        case (null) {
                            return;
                        };
                    }
                };
            };
            case (null) {}
        };
    };

    // DB operations //

    public type GetOptions = {superDB: SuperDB; subDBKey: SubDBKey; sk: SK};

    public func get(options: GetOptions) : ?AttributeValue {
        trapMoving({superDB = options.superDB; subDBKey = options.subDBKey});

        switch (getSubDB(options.superDB, options.subDBKey)) {
            case (?subDB) {
                RBT.get(subDB.data, Text.compare, options.sk);
            };
            case (null) {
                Debug.trap("missing sub-DB")
            }
        }
    };

    public type ExistsOptions = GetOptions;

    public func has(options: ExistsOptions) : Bool {
        get(options) != null;
    };

    public type HasSubDBOptions = {superDB: SuperDB; subDBKey: SubDBKey};

    public func hasSubDB(options: ExistsOptions) : Bool {
        trapMoving({superDB = options.superDB; subDBKey = options.subDBKey});

        BTree.has(options.superDB.subDBs, Nat.compare, options.subDBKey);
    };

    public func superDBSize(superDB: SuperDB): Nat = BTree.size(superDB.subDBs);

    public type SubDBSizeOptions = {superDB: SuperDB; subDBKey: SubDBKey};

    public func subDBSize(options: SubDBSizeOptions): ?Nat {
        switch (getSubDB(options.superDB, options.subDBKey)) {
            case (?subDB) { ?RBT.size(subDB.data) };
            case (null) { null }
        };
    };

    public type InsertOptions = {
        indexCanister: IndexCanister;
        currentCanister: PartitionCanister;
        superDB: SuperDB;
        subDBKey: SubDBKey;
        sk: SK;
        value: AttributeValue;
    };

    public func insert(options: InsertOptions) : async* () {
        trapMoving({superDB = options.superDB; subDBKey = options.subDBKey});

        switch (getSubDB(options.superDB, options.subDBKey)) {
            case (?subDB) {
                subDB.data := RBT.put(subDB.data, Text.compare, options.sk, options.value);
                removeLoosers(subDB);
                await* startMovingSubDBIfOverflow({
                    indexCanister = options.indexCanister;
                    oldCanister = options.currentCanister;
                    oldSuperDB = options.superDB;
                    oldSubDBKey = options.subDBKey
                });
            };
            case (null) {
                Debug.trap("missing sub-DB")
            }
        }
    };

    public type InsertOrCreateOptions = {
        indexCanister: IndexCanister;
        currentCanister: PartitionCanister;
        superDB: SuperDB;
        subDBKey: SubDBKey;
        sk: SK;
        value: AttributeValue;
        hardCap: ?Nat;
    };

    // FIXME: It creates a sub-DB and does not return its number.
    // FIXME: Not idempotent.
    public func insertOrCreate(options: InsertOrCreateOptions) : async* () {
        trapMoving({superDB = options.superDB; subDBKey = options.subDBKey});

        let subDB = switch (getSubDB(options.superDB, options.subDBKey)) {
            case (?subDB) {
                subDB;
            };
            case (null) {
                {
                    var data = RBT.init();
                    hardCap = options.hardCap;
                    var busy = false; // FIXME
                } : SubDB;
                // FIXME: This `SubDB` seems not inserted!
            };
        };
        subDB.data := RBT.put(subDB.data, Text.compare, options.sk, options.value);
        removeLoosers(subDB);
        await* startMovingSubDBIfOverflow({
            indexCanister = options.indexCanister;
            oldCanister = options.currentCanister;
            oldSuperDB = options.superDB;
            oldSubDBKey = options.subDBKey;
        });
    };

    // TODO: Here and in other places wrap `hardCap` into an object.
    public func createSubDB({superDB: SuperDB; hardCap: ?Nat; busy: Bool}) : Nat {
        let subDB : SubDB = {
            var data = RBT.init();
            hardCap = hardCap;
            var busy;
        };
        let key = superDB.nextKey;
        ignore BTree.insert(superDB.subDBs, Nat.compare, key, subDB);
        superDB.nextKey += 1;
        key;
    };

    type DeleteOptions = {superDB: SuperDB; subDBKey: SubDBKey; sk: SK};
    
    public func delete(options: DeleteOptions) {
        trapMoving({superDB = options.superDB; subDBKey = options.subDBKey});

        switch (getSubDB(options.superDB, options.subDBKey)) {
            case (?subDB) {
                subDB.data := RBT.delete<Text, AttributeValue>(subDB.data, Text.compare, options.sk);
            };
            case (null) {}; // TODO: trap?
        };
    };

    type DeleteSubDBOptions = {superDB: SuperDB; subDBKey: SubDBKey};
    
    public func deleteSubDB(options: DeleteSubDBOptions) {
        trapMoving({superDB = options.superDB; subDBKey = options.subDBKey});

        ignore BTree.delete(options.superDB.subDBs, Nat.compare, options.subDBKey);
    };

    // Scanning/enumerating //

    type IterOptions = {superDB: SuperDB; subDBKey: SubDBKey; dir : RBT.Direction};
    
    public func iter(options: IterOptions) : I.Iter<(Text, AttributeValue)> {
        switch (getSubDB(options.superDB, options.subDBKey)) {
            case (?subDB) {
                RBT.iter(subDB.data, options.dir);
            };
            case (null) {
                Debug.trap("missing sub-DB");
            };
        };
    };

    type EntriesOptions = {superDB: SuperDB; subDBKey: SubDBKey};
    
    public func entries(options: EntriesOptions) : I.Iter<(Text, AttributeValue)> {
        iter({superDB = options.superDB; subDBKey = options.subDBKey; dir = #fwd});
    };

    type EntriesRevOptions = EntriesOptions;
    
    public func entriesRev(options: EntriesRevOptions) : I.Iter<(Text, AttributeValue)> {
        iter({superDB = options.superDB; subDBKey = options.subDBKey; dir = #bwd});
    };

    public type ScanLimitResult = {
        results: [(Text, AttributeValue)];
        nextKey: ?Text;
    };

    type ScanLimitOptions = {superDB: SuperDB; subDBKey: SubDBKey; lowerBound: Text; upperBound: Text; dir: RBT.Direction; limit: Nat};
    
    public func scanLimit(options: ScanLimitOptions): RBT.ScanLimitResult<Text, AttributeValue> {
        switch (getSubDB(options.superDB, options.subDBKey)) {
            case (?subDB) {
                RBT.scanLimit(subDB.data, Text.compare, options.lowerBound, options.upperBound, options.dir, options.limit);
            };
            case (null) {
                Debug.trap("missing sub-DB");
            };
        };
    };
};