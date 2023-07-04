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
import Iter "mo:base/Iter";
import SparseQueue "../lib/SparseQueue";

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

    public type SubDB = {
        var data: RBT.Tree<SK, AttributeValue>;
        hardCap: ?Nat; // Remove "looser" items (with least key values) after reaching this count.
        var busy: Bool; // Forbid to move this entry to other canister.
                        // During the move it is true. Deletion in old canister and setting it to false happen in the same atomic action,
                        // so moving is also protected by this flag.
    };

    public type MoveCap = { #numDBs: Nat; #usedMemory: Nat };

    public type CreatingSubDB = {
        var canister: ?PartitionCanister;
    };

    /// Treat this as an opaque data structure, because this data is ignored if the sub-DB moves during insertion.
    public type InsertingItem = {
        part: PartitionCanister; // TODO: Can we remove this?
        subDBKey: SubDBKey;
    };

    public type SuperDB = {
        var nextKey: Nat;
        subDBs: BTree.BTree<SubDBKey, SubDB>;

        var moving: ?{
            oldCanister: PartitionCanister;
            oldSuperDB: SuperDB;
            oldSubDBKey: SubDBKey;
            var newCanister: ?{ // null - not yet determined
                canister: PartitionCanister;
                var newSubDBKey: ?SubDBKey; // null - not yet determined
            };
        };
        var inserting: SparseQueue.SparseQueue<InsertingItem>;
    };

    public type DBIndex = {
        var canisters: StableBuffer.StableBuffer<PartitionCanister>;
        var creatingSubDB: SparseQueue.SparseQueue<CreatingSubDB>;
    };

    public type IndexCanister = actor {
        getCanisters: query () -> async [PartitionCanister];
        newCanister(): async PartitionCanister;
        movingCallback: shared ({
            oldCanister: PartitionCanister;
            oldSubDBKey: SubDBKey;
            newCanister: PartitionCanister;
            newSubDBKey: SubDBKey;
        }) -> async ()
    };

    public type PartitionCanister = actor {
        rawInsertSubDB(data: RBT.Tree<SK, AttributeValue>, dbOptions: DBOptions) : async SubDBKey;
        isOverflowed({dbOptions: DBOptions}) : async Bool;
        superDBSize: query () -> async Nat;
        releaseSubDB(subDBKey: SubDBKey) : async (); // FIXME
        deleteSubDB({subDBKey: SubDBKey}) : async ();
        startInserting({subDBKey: SubDBKey; sk: SK; value: AttributeValue}) : async ();
        finishInserting(): async (PartitionCanister, SubDBKey);
        get: query (options: {subDBKey: SubDBKey; sk: SK}) -> async ?AttributeValue;
    };

    public func createDBIndex(options: {moveCap: MoveCap}) : DBIndex {
        {
            var canisters = StableBuffer.init<PartitionCanister>();
            var creatingSubDB = SparseQueue.init(100); // TODO
            moveCap = options.moveCap;
        }
    };

    public func createSuperDB() : SuperDB {
        {
            var nextKey = 0;
            subDBs = BTree.init<SubDBKey, SubDB>(null);
            var moving = null;
            var inserting = SparseQueue.init(100);
        }
    };

    public type MovingCallback = shared ({
        oldCanister: PartitionCanister;
        oldSubDBKey: SubDBKey;
        newCanister: PartitionCanister;
        newSubDBKey: SubDBKey;
    }) -> async ();

    public type DBOptions = {
        hardCap: ?Nat;
        moveCap: MoveCap;
        movingCallback: ?MovingCallback;
    };

    public func rawInsertSubDB(superDB: SuperDB, subDBData: RBT.Tree<SK, AttributeValue>, dbOptions: DBOptions): SubDBKey {
        switch (superDB.moving) {
            case (?_) { Debug.trap("DB is scaling") };
            case (null) {
                let key = superDB.nextKey;
                superDB.nextKey += 1;
                let subDB : SubDB = {
                    var data = subDBData;
                    hardCap = dbOptions.hardCap;
                    movingCallback = dbOptions.movingCallback;
                    var busy = false;
                };
                ignore BTree.insert(superDB.subDBs, Nat.compare, key, subDB);
                key;
            };
        };
    };

    public func getSubDB(superDB: SuperDB, subDBKey: SubDBKey) : ?SubDB {
        BTree.get(superDB.subDBs, Nat.compare, subDBKey);
    };

    public func releaseSubDB(superDB: SuperDB, subDBKey: SubDBKey) : async* () {
        switch (getSubDB(superDB, subDBKey)) {
            case (?item2) {
                item2.busy := false;
            };
            case (null) {};
        };
    };

    /// Moves to the specified `newCanister` or to a newly allocated canister, if null.
    func startMovingSubDBImpl(options: {oldCanister: PartitionCanister; newCanister: ?PartitionCanister; superDB: SuperDB; subDBKey: SubDBKey}) {
        switch (options.superDB.moving) {
            case (?_) { Debug.trap("already moving") };
            case (null) {
                options.superDB.moving := ?{
                    oldCanister = options.oldCanister;
                    oldSuperDB = options.superDB;
                    oldSubDBKey = options.subDBKey;
                    var newCanister = do ? {
                        {
                            canister = options.newCanister!;
                            var newSubDBKey = null;
                        };
                    };
                }
            };
        };
    };

    public func finishMovingSubDB({index: IndexCanister; oldSuperDB: SuperDB; dbOptions: DBOptions}) : async* ?(PartitionCanister, SubDBKey) {
        switch (oldSuperDB.moving) {
            case (?moving) {
                switch (BTree.get(moving.oldSuperDB.subDBs, Nat.compare, moving.oldSubDBKey)) {
                    case (?subDB) {
                        if (subDB.busy) {
                            Debug.trap("sub-DB is busy");
                        };
                        let (canister, newCanister) = switch (moving.newCanister) {
                            case (?newCanister) { (newCanister.canister, newCanister) };
                            case (null) {
                                let newCanister = await index.newCanister();
                                let s = {canister = newCanister; var newSubDBKey: ?SubDBKey = null};
                                moving.newCanister := ?s;
                                (newCanister, s);
                            };
                        };
                        let newSubDBKey = switch (newCanister.newSubDBKey) {
                            case (?newSubDBKey) { newSubDBKey };
                            case (null) {
                                let newSubDBKey = await canister.rawInsertSubDB(subDB.data, dbOptions);
                                newCanister.newSubDBKey := ?newSubDBKey;
                                newSubDBKey;
                            }
                        };                        
                        ignore BTree.delete(oldSuperDB.subDBs, Nat.compare, moving.oldSubDBKey); // FIXME: idempotent?
                        switch (dbOptions.movingCallback) {
                            case (?movingCallback) {
                                await movingCallback({
                                    oldCanister = moving.oldCanister;
                                    oldSubDBKey = moving.oldSubDBKey;
                                    newCanister = canister;
                                    newSubDBKey;
                                });
                            };
                            case (null) {};
                        };
                        subDB.busy := false;
                        oldSuperDB.moving := null;
                        return ?(canister, newSubDBKey);
                    };
                    case (null) {
                        return ?(moving.oldCanister, moving.oldSubDBKey);
                    };
                };
            };
            case (null) { null }; // may be called from `finishInserting`, so should not trap.
        };
    };

    func startMovingSubDB(options: {dbOptions: DBOptions; index: IndexCanister; oldCanister: PartitionCanister; oldSuperDB: SuperDB; oldSubDBKey: SubDBKey}) : async* () {
        let ?item = BTree.get(options.oldSuperDB.subDBs, Nat.compare, options.oldSubDBKey) else {
            Debug.trap("item must exist");
        };
        if (item.busy) {
            Debug.trap("is moving");
        };
        item.busy := true;
        let pks = await options.index.getCanisters();
        let lastCanister = pks[pks.size()-1];
        if (lastCanister == options.oldCanister or (await lastCanister.isOverflowed({dbOptions = options.dbOptions}))) {
            startMovingSubDBImpl({
                oldCanister = options.oldCanister;
                superDB = options.oldSuperDB;
                subDBKey = options.oldSubDBKey;
                newCanister = null;
            });
        } else {
            startMovingSubDBImpl({
                oldCanister = options.oldCanister;
                superDB = options.oldSuperDB;
                subDBKey = options.oldSubDBKey;
                newCanister = ?lastCanister;
            });
        };
    };

    public func isOverflowed({dbOptions: DBOptions; superDB: SuperDB}) : Bool {
        switch (dbOptions.moveCap) {
            case (#numDBs num) {
                BTree.size(superDB.subDBs) > num;
            };
            case (#usedMemory mem) {
                Prim.rts_heap_size() > mem; // current canister
            };
        };
    };

    func startMovingSubDBIfOverflow(
        options: {indexCanister: IndexCanister; oldCanister: PartitionCanister; oldSuperDB: SuperDB; oldSubDBKey: SubDBKey;
            dbOptions: DBOptions}): async* ()
    {
        if (await options.oldCanister.isOverflowed({dbOptions = options.dbOptions})) { // TODO: Remove `moveCap`
            await* startMovingSubDB({
                dbOptions = options.dbOptions;
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

    public type HasDBOptions = {superDB: SuperDB; subDBKey: SubDBKey};

    public func hasSubDB(options: HasDBOptions) : Bool {
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
        dbOptions: DBOptions;
        indexCanister: IndexCanister;
        currentCanister: PartitionCanister;
        superDB: SuperDB;
        subDBKey: SubDBKey;
        sk: SK;
        value: AttributeValue;
    };

    public func startInserting(options: InsertOptions) : async* Nat {
        trapMoving({superDB = options.superDB; subDBKey = options.subDBKey});

        switch (getSubDB(options.superDB, options.subDBKey)) {
            case (?subDB) {
                subDB.data := RBT.put(subDB.data, Text.compare, options.sk, options.value);
                removeLoosers(subDB);

                let insertId = SparseQueue.add<InsertingItem>(options.superDB.inserting, {
                    part = options.currentCanister;
                    subDBKey = options.subDBKey;
                });

                await* startMovingSubDBIfOverflow({
                    dbOptions = options.dbOptions;
                    indexCanister = options.indexCanister;
                    oldCanister = options.currentCanister;
                    oldSuperDB = options.superDB;
                    oldSubDBKey = options.subDBKey
                });

                insertId;
            };
            case (null) {
                Debug.trap("missing sub-DB");
            };
        };
    };

    public func finishInserting({index: IndexCanister; oldSuperDB: SuperDB; dbOptions: DBOptions; insertId: SparseQueue.SparseQueueKey}): async* (PartitionCanister, SubDBKey) {
        let ?v = SparseQueue.get(oldSuperDB.inserting, insertId) else {
            Debug.trap("not inserting");
        };
        let (part, subDBKey) = switch(await* finishMovingSubDB({index; oldSuperDB; dbOptions})) {
            case (?(part, subDBKey)) { (part, subDBKey) };
            case (null) {
                let x = SparseQueue.get(oldSuperDB.inserting, insertId);
                let ?{part; subDBKey} = x else {
                    Debug.trap("not inserting");
                };
                (part, subDBKey)
            }
        };
        SparseQueue.delete(oldSuperDB.inserting, insertId);
        (part, subDBKey);
    };

    type DeleteOptions = {superDB: SuperDB; subDBKey: SubDBKey; sk: SK};
    
    public func delete(options: DeleteOptions) {
        trapMoving({superDB = options.superDB; subDBKey = options.subDBKey});

        switch (getSubDB(options.superDB, options.subDBKey)) {
            case (?subDB) {
                subDB.data := RBT.delete(subDB.data, Text.compare, options.sk);
            };
            case (null) {};
        };
    };

    type DeleteDBOptions = {superDB: SuperDB; subDBKey: SubDBKey};
    
    public func deleteSubDB(options: DeleteDBOptions) {
        trapMoving({superDB = options.superDB; subDBKey = options.subDBKey});

        ignore BTree.delete(options.superDB.subDBs, Nat.compare, options.subDBKey);
    };

    // Creating sub-DB //

    // It does not touch old items, so no locking.
    public func startCreatingSubDB({dbIndex: DBIndex; dbOptions: DBOptions}): async* Nat {
        if (StableBuffer.size(dbIndex.canisters) == 0) {
            Debug.trap("no partition canisters");
        };
        SparseQueue.add<CreatingSubDB>(dbIndex.creatingSubDB, {var canister = null});
    };

    public func finishCreatingSubDB({index: IndexCanister; dbIndex: DBIndex; dbOptions: DBOptions; creatingId: Nat})
        : async* (PartitionCanister, SubDBKey)
    {
        switch (SparseQueue.get(dbIndex.creatingSubDB, creatingId)) {
            case (?creating) {
                let part: PartitionCanister = switch (creating.canister) {
                    case (?part) { part };
                    case (null) {
                        switch (dbOptions.moveCap) {
                            case (#numDBs n) {
                                let part = StableBuffer.get(dbIndex.canisters, StableBuffer.size(dbIndex.canisters) - 1);
                                if ((await part.superDBSize()) >= n) {
                                    await index.newCanister();
                                } else {
                                    part
                                };
                            };
                            case (#usedMemory m) {
                                var part = StableBuffer.get(dbIndex.canisters, StableBuffer.size(dbIndex.canisters) - 1);
                                // Trial creation...
                                let subDBKey = await part.rawInsertSubDB(RBT.init(), dbOptions); // We don't need `busy == true`, because we didn't yet created "links" to it.
                                creating.canister := ?part;
                                if (await part.isOverflowed({dbOptions})) { // TODO: Join .isOverflowed and .deleteSubDB into one call?
                                    // ... with possible deletion afterward.
                                    await part.deleteSubDB({subDBKey});
                                    part := await index.newCanister();
                                    creating.canister := ?part;
                                } else {
                                    SparseQueue.delete(dbIndex.creatingSubDB, creatingId);
                                    return (part, subDBKey);
                                };
                                part;
                            };
                        };
                    };
                };
                let subDBKey = await part.rawInsertSubDB(RBT.init(), dbOptions); // We don't need `busy == true`, because we didn't yet created "links" to it.
                SparseQueue.delete(dbIndex.creatingSubDB, creatingId);
                (part, subDBKey);
            };
            case (null) {
                Debug.trap("not creating");
            };
        };
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