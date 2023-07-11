import I "mo:base/Iter";
import Principal "mo:base/Principal";
import BTree "mo:btree/BTree";
import RBT "mo:stable-rbtree/StableRBTree";
import StableBuffer "mo:stable-buffer/StableBuffer";
import Text "mo:base/Text";
import Nat "mo:base/Nat";
import Int "mo:base/Int";
import Prim "mo:⛔";
import Buffer "mo:base/Buffer";
import Debug "mo:base/Debug";
import Bool "mo:base/Bool";
import Deque "mo:base/Deque";
import Iter "mo:base/Iter";
import SparseQueue "../lib/SparseQueue";

module {
    /// The key under which a sub-DB stored in a canister.
    public type InwardSubDBKey = Nat;

    /// Constant (regarding moving a sub-DB to another canister) key mapped to `InwardSubDBKey`.
    public type OutwardSubDBKey = Nat;

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
        var map: RBT.Tree<SK, AttributeValue>;
        var userData: Text; // useful to have a back reference to "locator" of our sub-DB in another database
        var busy: Bool; // Forbid to move this entry to other canister.
                        // During the move it is true. Deletion in old canister and setting it to false happen in the same atomic action,
                        // so moving is also protected by this flag.
    };

    public type MoveCap = { #usedMemory: Nat };

    public type CreatingSubDB = {
        var canister: ?PartitionCanister;
        userData: Text;
    };

    /// Treat this as an opaque data structure, because this data is ignored if the sub-DB moves during insertion.
    public type InsertingItem = {
        part: PartitionCanister; // TODO: Can we remove this?
        subDBKey: SubDBKey;
    };

    public type SuperDB = {
        var nextInwardKey: Nat;
        var nextOutwardKey: Nat;
        subDBs: BTree.BTree<SubDBKey, SubDB>;
        /// The canister and the `SubDBKey` of this `RBT.Tree` is constant,
        /// even when the sub-DB to which it points moves to a different canister.
        var locations: RBT.Tree<SubDBKey, (PartitionCanister, SubDBKey)>;

        var moving: ?{ // TODO: delete?
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
        movingCallback: shared ({ // TODO: delete?
            oldCanister: PartitionCanister;
            oldSubDBKey: SubDBKey;
            newCanister: PartitionCanister;
            newSubDBKey: SubDBKey;
            userData: Text;
        }) -> async ();
        startCreatingSubDB: shared({dbOptions: DBOptions; userData: Text}) -> async Nat;
        finishCreatingSubDB: shared({index: IndexCanister; dbOptions: DBOptions; creatingId: Nat})
            -> async (PartitionCanister, SubDBKey);
    };

    public type PartitionCanister = actor {
        rawInsertSubDB(map: RBT.Tree<SK, AttributeValue>, userData: Text, dbOptions: DBOptions) : async SubDBKey;
        isOverflowed({dbOptions: DBOptions}) : async Bool;
        superDBSize: query () -> async Nat;
        deleteSubDB({subDBKey: SubDBKey}) : async ();
        startInserting({subDBKey: SubDBKey; sk: SK; value: AttributeValue}) : async SparseQueue.SparseQueueKey;
        finishInserting({dbOptions : DBOptions; index : IndexCanister; insertId : SparseQueue.SparseQueueKey})
            : async (PartitionCanister, SubDBKey);
        get: query (options: {subDBKey: SubDBKey; sk: SK}) -> async ?AttributeValue;
        has: query (options: {subDBKey: SubDBKey; sk: SK}) -> async Bool;
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
            var locations = RBT.init();
            var moving = null;
            var inserting = SparseQueue.init(100);
        }
    };

    // TODO: Delete.
    public type MovingCallback = shared ({
        oldCanister: PartitionCanister;
        oldSubDBKey: SubDBKey;
        newCanister: PartitionCanister;
        newSubDBKey: SubDBKey;
        userData: Text;
    }) -> async ();

    public type DBOptions = {
        hardCap: ?Nat;
        moveCap: MoveCap;
        movingCallback: ?MovingCallback; // TODO: Delete.
    };

    /// The "real" returned value is `outward`, but `inward` can be used for caching
    /// (on cache failure retrieve new `inward` using `outward`).
    public func rawInsertSubDB(part: PartitionCanister, superDB: SuperDB, map: RBT.Tree<SK, AttributeValue>, userData: Text, dbOptions: DBOptions)
        : {outward: OutwardSubDBKey; inward: InwardSubDBKey}
    {
        let inward = switch (superDB.moving) {
            case (?_) { Debug.trap("DB is scaling") };
            case (null) {
                let key = superDB.nextInwardKey;
                superDB.nextInwardKey += 1;
                let subDB : SubDB = {
                    var map = map;
                    var userData = userData;
                    hardCap = dbOptions.hardCap;
                    movingCallback = dbOptions.movingCallback;
                    var busy = false;
                };
                ignore BTree.insert(superDB.subDBs, Nat.compare, key, subDB);
                key;
            };
        };
        // We always insert the location to the same canister as the sub-DB.
        // (Later sub-DB may be moved to another canister.)
        superDB.locations := RBT.insert(superDB.locations, superDB.nextOutwardKey, Nat.compare, (part, inward));
        let result = {outward: superDB.nextOutwardKey; inward};
        superDB.nextOutwardKey += 1;
    };

    public func getInward(superDB: SuperDB, outwardKey: InwardSubDBKey) : ?InwardSubDBKey {
        RBT.get(superDB.locations, Nat.compare, outwardKey);
    };

    public func getSubDBByInward(superDB: SuperDB, subDBKey: InwardSubDBKey) : ?SubDB {
        BTree.get(superDB.subDBs, Nat.compare, subDBKey);
    };

    /// This function makes no sense, because it would return the entire sub-DB from another canister.
    // public func getSubDBByOutward(superDB: SuperDB, subDBKey: OutwardSubDBKey) : ?SubDB {
    // };

    // TODO: remove?
    /// Moves to the specified `newCanister` or to a newly allocated canister, if null.
    // func startMovingSubDBImpl(options: {oldCanister: PartitionCanister; newCanister: ?PartitionCanister; superDB: SuperDB; subDBKey: SubDBKey}) {
    //     switch (options.superDB.moving) {
    //         case (?_) { Debug.trap("already moving") };
    //         case (null) {
    //             options.superDB.moving := ?{
    //                 oldCanister = options.oldCanister;
    //                 oldSuperDB = options.superDB;
    //                 oldSubDBKey = options.subDBKey;
    //                 var newCanister = do ? {
    //                     {
    //                         canister = options.newCanister!;
    //                         var newSubDBKey = null;
    //                     };
    //                 };
    //             }
    //         };
    //     };
    // };

    // TODO: remove?
    // public func finishMovingSubDB({index: IndexCanister; oldSuperDB: SuperDB; dbOptions: DBOptions}) : async* ?(PartitionCanister, SubDBKey) {
    //     switch (oldSuperDB.moving) {
    //         case (?moving) {
    //             switch (BTree.get(moving.oldSuperDB.subDBs, Nat.compare, moving.oldSubDBKey)) {
    //                 case (?subDB) {
    //                     let (canister, newCanister) = switch (moving.newCanister) {
    //                         case (?newCanister) { (newCanister.canister, newCanister) };
    //                         case (null) {
    //                             let newCanister = await index.newCanister();
    //                             let s = {canister = newCanister; var newSubDBKey: ?SubDBKey = null};
    //                             moving.newCanister := ?s;
    //                             (newCanister, s);
    //                         };
    //                     };
    //                     let newSubDBKey = switch (newCanister.newSubDBKey) {
    //                         case (?newSubDBKey) { newSubDBKey };
    //                         case (null) {
    //                             let newSubDBKey = await canister.rawInsertSubDB(subDB.map, subDB.userData, dbOptions);
    //                             newCanister.newSubDBKey := ?newSubDBKey;
    //                             newSubDBKey;
    //                         }
    //                     };                        
    //                     ignore BTree.delete(oldSuperDB.subDBs, Nat.compare, moving.oldSubDBKey); // FIXME: idempotent?
    //                     switch (dbOptions.movingCallback) {
    //                         case (?movingCallback) {
    //                             await movingCallback({
    //                                 oldCanister = moving.oldCanister;
    //                                 oldSubDBKey = moving.oldSubDBKey;
    //                                 newCanister = canister;
    //                                 newSubDBKey;
    //                                 userData = subDB.userData;
    //                             });
    //                         };
    //                         case (null) {};
    //                     };
    //                     subDB.busy := false;
    //                     oldSuperDB.moving := null;
    //                     return ?(canister, newSubDBKey);
    //                 };
    //                 case (null) {
    //                     return ?(moving.oldCanister, moving.oldSubDBKey);
    //                 };
    //             };
    //         };
    //         case (null) { null }; // may be called from `finishInserting`, so should not trap.
    //     };
    // };

    // TODO: remove?
    // func startMovingSubDB(options: {dbOptions: DBOptions; index: IndexCanister; oldCanister: PartitionCanister; oldSuperDB: SuperDB; oldSubDBKey: SubDBKey}) : async* () {
    //     let ?item = BTree.get(options.oldSuperDB.subDBs, Nat.compare, options.oldSubDBKey) else {
    //         Debug.trap("item must exist");
    //     };
    //     if (item.busy) {
    //         Debug.trap("is moving");
    //     };
    //     item.busy := true;
    //     let pks = await options.index.getCanisters();
    //     let lastCanister = pks[pks.size()-1];
    //     if (lastCanister == options.oldCanister or (await lastCanister.isOverflowed({dbOptions = options.dbOptions}))) {
    //         startMovingSubDBImpl({
    //             oldCanister = options.oldCanister;
    //             superDB = options.oldSuperDB;
    //             subDBKey = options.oldSubDBKey;
    //             newCanister = null;
    //         });
    //     } else {
    //         startMovingSubDBImpl({
    //             oldCanister = options.oldCanister;
    //             superDB = options.oldSuperDB;
    //             subDBKey = options.oldSubDBKey;
    //             newCanister = ?lastCanister;
    //         });
    //     };
    // };

    public func isOverflowed({dbOptions: DBOptions; superDB: SuperDB}) : Bool {
        switch (dbOptions.moveCap) {
            case (#usedMemory mem) {
                Prim.rts_heap_size() > mem; // current canister
            };
        };
    };

    // TODO?
    func startMovingSubDBIfOverflow(
        options: {indexCanister: IndexCanister; oldCanister: PartitionCanister; oldSuperDB: SuperDB; oldSubDBKey: SubDBKey;
            dbOptions: DBOptions}): async* ()
    {
        if (await options.oldCanister.isOverflowed({dbOptions = options.dbOptions})) {
            await* startMovingSubDB({
                dbOptions = options.dbOptions;
                index = options.indexCanister;
                oldCanister = options.oldCanister;
                oldSuperDB = options.oldSuperDB;
                oldSubDBKey = options.oldSubDBKey;
            });
        }
    };

    // TODO?
    func trapMoving({superDB: SuperDB; subDBKey: SubDBKey}) {
        switch (BTree.get(superDB.subDBs, Nat.compare, subDBKey)) {
            case (?item) {
                if (item.busy) {
                    Debug.trap("item busy");
                }
            };
            case (null) {};
        };
    };

    func removeLoosers({subDB: SubDB; dbOptions: DBOptions}) {
        switch (dbOptions.hardCap) {
            case (?hardCap) {
                while (RBT.size(subDB.map) > hardCap) {
                    let iter = RBT.entries(subDB.map);
                    switch (iter.next()) {
                        case (?(k, v)) {
                            subDB.map := RBT.delete(subDB.map, Text.compare, k);
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

    public func getByInward(options: GetOptions) : ?AttributeValue {
        trapMoving({superDB = options.superDB; subDBKey = options.subDBKey}); // TODO: here and in other places

        switch (getSubDB(options.superDB, options.subDBKey)) {
            case (?subDB) {
                RBT.get(subDB.map, Text.compare, options.sk);
            };
            case (null) {
                Debug.trap("missing sub-DB")
            }
        }
    };

    public func getByOutward(options: GetOptions) : async* ?AttributeValue {
        let ?(part, inward) = getInward(superDB: SuperDB, outwardKey) else {
            Debug.trap("no entry");
        };
        await part.getByInward(inward);
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

    public type GetUserDataOptions = {superDB: SuperDB; subDBKey: SubDBKey};

    // TODO: Test this function
    public func getSubDBUserData(options: GetUserDataOptions) : ?Text {
        trapMoving({superDB = options.superDB; subDBKey = options.subDBKey});

        do ? { BTree.get(options.superDB.subDBs, Nat.compare, options.subDBKey)!.userData };
    };

    public func superDBSize(superDB: SuperDB): Nat = BTree.size(superDB.subDBs);

    public type SubDBSizeOptions = {superDB: SuperDB; subDBKey: SubDBKey};

    public func subDBSize(options: SubDBSizeOptions): ?Nat {
        switch (getSubDB(options.superDB, options.subDBKey)) {
            case (?subDB) { ?RBT.size(subDB.map) };
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
                subDB.map := RBT.put(subDB.map, Text.compare, options.sk, options.value);
                removeLoosers({subDB; dbOptions = options.dbOptions});

                let insertId = SparseQueue.add<InsertingItem>(options.superDB.inserting, {
                    part = options.currentCanister;
                    subDBKey = options.subDBKey;
                });

                // TODO: Check only in the case of memory cap, not number of DBs cap:
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
                subDB.map := RBT.delete(subDB.map, Text.compare, options.sk);
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
    public func startCreatingSubDB({dbIndex: DBIndex; dbOptions: DBOptions; userData: Text}): async* Nat {
        if (StableBuffer.size(dbIndex.canisters) == 0) {
            Debug.trap("no partition canisters");
        };
        SparseQueue.add<CreatingSubDB>(dbIndex.creatingSubDB, {var canister = null; userData});
    };

    /// In the current version two partition canister are always the same.
    ///
    /// `superDB` should reside in `part`.
    func bothKeys(superDB: SuperDB, part: PartitionCanister, inwardKey: InwardSubDBKey)
        : {inward: (PartitionCanister, InwardSubDBKey); outward: (PartitionCanister, OutwardSubDBKey)}
    {
        superDB.locations := RBT.insert(superDB.locations, superDB.nextOutwardKey, Nat.compare, (part, inward));
        let result = {inward: (part, inward); outward: (part, superDB.nextOutwardKey)};
        superDB.nextOutwardKey += 1;
        result;
    };

    /// The "real" returned value is `outward`, but `inward` can be used for caching
    /// (on cache failure retrieve new `inward` using `outward`).
    ///
    /// In this version returned `PartitionCanister` for inward and outward always the same.
    public func finishCreatingSubDB({index: IndexCanister; dbIndex: DBIndex; dbOptions: DBOptions; creatingId: Nat})
        : async* {inward: (PartitionCanister, InwardSubDBKey); outward: (PartitionCanister, OutwardSubDBKey)}
    {
        switch (SparseQueue.get(dbIndex.creatingSubDB, creatingId)) {
            case (?creating) {
                let part: PartitionCanister = switch (creating.canister) {
                    case (?part) { part };
                    case (null) {
                        if (await part.isOverflowed({dbOptions})) { // TODO: Join .isOverflowed and .deleteSubDB into one call?
                            part := await index.newCanister();
                            creating.canister := ?part;
                        } else {
                            SparseQueue.delete(dbIndex.creatingSubDB, creatingId);
                            return bothKeys(part, subDBKey);
                        };
                        part;
                    };
                };
                let inwardKey = await part.rawInsertSubDB(RBT.init(), creating.userData, dbOptions); // We don't need `busy == true`, because we didn't yet created "links" to it.
                SparseQueue.delete(dbIndex.creatingSubDB, creatingId);
                bothKeys(part, inwardKey);
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
                RBT.iter(subDB.map, options.dir);
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
                RBT.scanLimit(subDB.map, Text.compare, options.lowerBound, options.upperBound, options.dir, options.limit);
            };
            case (null) {
                Debug.trap("missing sub-DB");
            };
        };
    };
};