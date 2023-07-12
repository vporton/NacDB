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
        subDBKey: OutwardSubDBKey;
    };

    public type SuperDB = {
        var nextInwardKey: Nat;
        var nextOutwardKey: Nat;
        subDBs: BTree.BTree<InwardSubDBKey, SubDB>;
        /// The canister and the `SubDBKey` of this `RBT.Tree` is constant,
        /// even when the sub-DB to which it points moves to a different canister.
        var locations: RBT.Tree<OutwardSubDBKey, (PartitionCanister, InwardSubDBKey)>;

        var moving: ?{
            // outwardSuperDB: SuperDB; // cannot be passed together with `oldInwardSuperDB`...
            outwardCanister: PartitionCanister; // ... so, this instead.
            outwardKey: OutwardSubDBKey;
            oldInwardCanister: PartitionCanister;
            oldInwardSuperDB: SuperDB;
            oldInwardSubDBKey: InwardSubDBKey;
            var newInwardCanister: ?{ // null - not yet determined
                canister: PartitionCanister;
                var newSubDBKey: ?InwardSubDBKey; // null - not yet determined
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
        startCreatingSubDB: shared({dbOptions: DBOptions; userData: Text}) -> async Nat;
        finishCreatingSubDB: shared({index: IndexCanister; dbOptions: DBOptions; creatingId: Nat})
            -> async {inward: (PartitionCanister, InwardSubDBKey); outward: (PartitionCanister, OutwardSubDBKey)};
    };

    public type PartitionCanister = actor {
        rawInsertSubDB(map: RBT.Tree<SK, AttributeValue>, userData: Text, dbOptions: DBOptions) : async InwardSubDBKey;
        isOverflowed({dbOptions: DBOptions}) : async Bool;
        superDBSize: query () -> async Nat;
        deleteSubDB({subDBKey: OutwardSubDBKey}) : async ();
        startInserting({subDBKey: OutwardSubDBKey; sk: SK; value: AttributeValue}) : async SparseQueue.SparseQueueKey;
        finishInserting({dbOptions : DBOptions; index : IndexCanister; insertId : SparseQueue.SparseQueueKey})
            : async {inward: (PartitionCanister, InwardSubDBKey); outward: (PartitionCanister, OutwardSubDBKey)};
        getByInward: query (options: {subDBKey: InwardSubDBKey; sk: SK}) -> async ?AttributeValue;
        hasByInward: query (options: {subDBKey: InwardSubDBKey; sk: SK}) -> async Bool;
        getByOutward: query (options: {subDBKey: OutwardSubDBKey; sk: SK}) -> async ?AttributeValue;
        hasByOutward: query (options: {subDBKey: OutwardSubDBKey; sk: SK}) -> async Bool;
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

    public type DBOptions = {
        hardCap: ?Nat;
        moveCap: MoveCap;
    };

    /// The "real" returned value is `outward`, but `inward` can be used for caching
    /// (on cache failure retrieve new `inward` using `outward`).
    public func rawInsertSubDB(part: PartitionCanister, superDB: SuperDB, map: RBT.Tree<SK, AttributeValue>, userData: Text, dbOptions: DBOptions)
        : {outward: OutwardSubDBKey; inward: InwardSubDBKey}
    {
        let inward = switch (superDB.moving) {
            case (?_) { Debug.trap("DB is scaling") }; // TODO: needed?
            case (null) {
                let key = superDB.nextInwardKey;
                superDB.nextInwardKey += 1;
                let subDB : SubDB = {
                    var map = map;
                    var userData = userData;
                    hardCap = dbOptions.hardCap;
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

    public func putLocation(outwardSuperDB: SuperDB, outwardKey: OutwardSubDBKey, inwardCanister: PartitionCanister, inwardKey: InwardSubDBKey) {
        outwardSuperDB.locations := RBT.put(outwardSuperDB.locations, Nat.compare, outwardKey, (inwardCanister, inwardKey));
    };

    /// This function makes no sense, because it would return the entire sub-DB from another canister.
    // public func getSubDBByOutward(superDB: SuperDB, subDBKey: OutwardSubDBKey) : ?SubDB {
    // };

    /// Moves to the specified `newCanister` or to a newly allocated canister, if null.
    func startMovingSubDBImpl({
        outwardCanister: PartitionCanister; // ... so, this instead.
        outwardKey: OutwardSubDBKey;
        oldInwardCanister: PartitionCanister;
        oldInwardSuperDB: SuperDB;
        oldInwardSubDBKey: InwardSubDBKey;
        newCanister: ?PartitionCanister;
    }) {
        switch (options.superDB.moving) {
            case (?_) { Debug.trap("already moving") };
            case (null) {
                options.superDB.moving := ?{
                    outwardCanister;
                    outwardKey;
                    oldInwardCanister;
                    oldInwardSuperDB;
                    oldInwardSubDBKey;
                    var newInwardCanister = do ? {
                        {
                            canister = options.newCanister!;
                            var newSubDBKey = null;
                        };
                    };
                };
            };
        };
    };

    // FIXME: arguments for inward/outward
    // FIXME: What to do with the case if the new sub-DB is already created and the old is not yet deleted? (Needs cleanup)
    public func finishMovingSubDB({index: IndexCanister; oldSuperDB: SuperDB; dbOptions: DBOptions}) : async* ?(PartitionCanister, InwardSubDBKey) {
        switch (oldSuperDB.moving) {
            case (?moving) {
                switch (BTree.get(moving.oldSuperDB.subDBs, Nat.compare, moving.oldSubDBKey)) {
                    case (?subDB) {
                        let (canister, newCanister) = switch (moving.newCanister) {
                            case (?newCanister) { (newCanister.canister, newCanister) };
                            case (null) {
                                let newCanister = await index.newCanister();
                                let s = {canister = newCanister; var newSubDBKey: ?SubDBKey = null};
                                moving.newCanister := ?s;
                                (newCanister, s);
                            };
                        };
                        let newInwardSubDBKey = switch (newCanister.newSubDBKey) {
                            case (?newSubDBKey) { newSubDBKey };
                            case (null) {
                                let newSubDBKey = await canister.rawInsertSubDB(subDB.map, subDB.userData, dbOptions);
                                newCanister.newSubDBKey := ?newSubDBKey;
                                newSubDBKey;
                            }
                        };                        
                        ignore BTree.delete(oldSuperDB.subDBs, Nat.compare, moving.oldSubDBKey); // FIXME: idempotent?
                        await outwardCanister.putLocation(outwardKey, moving.newInwardSubDBKey);
                        subDB.busy := false;
                        oldSuperDB.moving := null;
                        return ?(canister, newInwardSubDBKey); // TODO: need to return inward key?
                    };
                    case (null) {
                        return ?(moving.oldCanister, moving.oldSubDBKey);
                    };
                };
            };
            case (null) { null }; // may be called from `finishInserting`, so should not trap.
        };
    };

    func startMovingSubDB(options: {
        dbOptions: DBOptions;
        index: IndexCanister;
        oldCanister: PartitionCanister;
        oldSuperDB: SuperDB;
        oldSubDBKey: InwardSubDBKey;
    }) : async* () {
        let ?item = BTree.get(options.oldSuperDB.subDBs, Nat.compare, options.oldSubDBKey) else {
            Debug.trap("item must exist");
        };
        if (item.busy) {
            Debug.trap("is moving");
        };
        item.busy := true; // TODO: seems superfluous
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
            case (#usedMemory mem) {
                Prim.rts_heap_size() > mem; // current canister
            };
        };
    };

    func startMovingSubDBIfOverflow(
        options: {
            dbOptions: DBOptions;
            index: IndexCanister;
            oldCanister: PartitionCanister;
            oldSuperDB: SuperDB;
            oldSubDBKey: InwardSubDBKey;
        }): async* ()
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
    // func trapMoving({superDB: SuperDB; subDBKey: SubDBKey}) {
    //     switch (BTree.get(superDB.subDBs, Nat.compare, subDBKey)) {
    //         case (?item) {
    //             if (item.busy) {
    //                 Debug.trap("item busy");
    //             }
    //         };
    //         case (null) {};
    //     };
    // };

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

    public type GetByInwardOptions = {superDB: SuperDB; subDBKey: InwardSubDBKey; sk: SK};

    public func getByInward(options: GetByInwardOptions) : ?AttributeValue {
        switch (getSubDB(options.superDB, options.subDBKey)) {
            case (?subDB) {
                RBT.get(subDB.map, Text.compare, options.sk);
            };
            case (null) {
                Debug.trap("missing sub-DB")
            }
        }
    };

    public type GetByOutwardOptions = {outwardSuperDB: SuperDB; subDBKey: OutwardSubDBKey; sk: SK};

    public func getByOutward(options: GetByOutwardOptions) : async* ?AttributeValue {
        let ?(part, inward) = getInward(outwardSuperDB, outwardKey) else {
            Debug.trap("no entry");
        };
        await part.getByInward(inward);
    };

    public type ExistsByInwardOptions = GetByInwardOptions;

    public func hasByInward(options: ExistsByInwardOptions) : Bool {
        getByInward(options) != null;
    };

    public type ExistsByOutwardOptions = GetByOutwardOptions;

    public func hasByOutward(options: ExistsByOutwardOptions) : Bool {
        getByOutward(options) != null;
    };

    public type HasDBByInwardOptions = {inwardSuperDB: SuperDB; inwardKey: InwardSubDBKey};

    public func hasSubDBByInward(options: HasDBByInwardOptions) : Bool {
        BTree.has(options.inwardSuperDB.subDBs, Nat.compare, options.subDBKey);
    };

    public type HasDBByOutwardOptions = {outwardSuperDB: SuperDB; outwardKey: OutwardSubDBKey};

    public func hasSubDBByOutward(options: HasDBByOutwardOptions) : Bool {
        let subDB = RBT.get(options.outwardSuperDB, options.outwardKey, Nat.compare);
        subDB != null and (do ? {
            BTree.has(subDB!, Nat.compare, options.subDBKey);
        });
    };

    // TODO: This inward and outward.
    // public type GetUserDataOptions = {superDB: SuperDB; subDBKey: SubDBKey};

    // // TODO: Test this function
    // public func getSubDBUserData(options: GetUserDataOptions) : ?Text {
    //     do ? { BTree.get(options.superDB.subDBs, Nat.compare, options.subDBKey)!.userData };
    // };

    public func superDBSize(superDB: SuperDB): Nat = BTree.size(superDB.subDBs);

    public type SubDBSizeByInwardOptions = {superDB: SuperDB; subDBKey: InwardSubDBKey};

    public func subDBSizeByInward(options: SubDBSizeByInwardOptions): ?Nat {
        do ? {
            ?RBT.size(getSubDB(options.superDB, options.subDBKey)!.map);
        }
    };

    public type SubDBSizeByOutwardOptions = {superDB: SuperDB; subDBKey: OutwardSubDBKey};

    public func subDBSizeByOutward(options: SubDBSizeByOutwardOptions): ?Nat {
        let ?(part, inwardKey) = RBT.get(options.outwardSuperDB, options.outwardKey, Nat.compare) else {
            Debug.trap("no sub-DB");
        };
        do ? {
            subDBSizeByInward(part, inwardKey);
        };
    };

    public type InsertOptions = {
        dbOptions: DBOptions;
        indexCanister: IndexCanister;
        outwardCanister: PartitionCanister;
        outwardSuperDB: SuperDB;
        outwardKey: OutwardSubDBKey;
        sk: SK;
        value: AttributeValue;
    };

    public func startInserting(options: InsertOptions) : async* Nat {
        switch (getSubDB(options.superDB, options.subDBKey)) {
            case (?subDB) {
                subDB.map := RBT.put(subDB.map, Text.compare, options.sk, options.value);
                removeLoosers({subDB; dbOptions = options.dbOptions});

                let insertId = SparseQueue.add<InsertingItem>(options.superDB.inserting, {
                    part = options.outwardCanister;
                    subDBKey = options.outwardKey;
                });

                await* startMovingSubDBIfOverflow({
                    dbOptions = options.dbOptions;
                    indexCanister = options.indexCanister;
                    oldCanister = options.outwardCanister;
                    oldSuperDB = options.outwardSuperDB;
                    oldSubDBKey = options.subDBKey; // FIXME
                });

                insertId;
            };
            case (null) {
                Debug.trap("missing sub-DB");
            };
        };
    };

    public func finishInserting({index: IndexCanister; oldSuperDB: SuperDB; dbOptions: DBOptions; insertId: SparseQueue.SparseQueueKey})
        : async* {inward: (PartitionCanister, InwardSubDBKey); outward: (PartitionCanister, OutwardSubDBKey)}
    {
        let ?v = SparseQueue.get(oldSuperDB.inserting, insertId) else {
            Debug.trap("not inserting");
        };
        let (part, subDBKey) = switch(await* finishMovingSubDB({index; oldSuperDB; dbOptions})) {
            case (?(part, subDBKey)) { (part, subDBKey) };
            case (null) {
                let ?{part; subDBKey} = SparseQueue.get(oldSuperDB.inserting, insertId) else {
                    Debug.trap("not inserting");
                };
                bothKeys(part, subDBKey)
            }
        };
        SparseQueue.delete(oldSuperDB.inserting, insertId);
        bothKeys(part, subDBKey);
    };

    type DeleteOptions = {outwardSuperDB: SuperDB; outwardKey: OutwardSubDBKey; sk: SK};
    
    /// idempotent
    public func delete(options: DeleteOptions) {
        switch(getInward({outwardSuperDB; outwardKey})) {
            case (?(inwardCanister, inwardKey)) {
                inwardCanister.deleteInward(inwardKey, options.sk);
            };
            case (null) {};
        };
        options.outwardSuperDB.locations := BTree.delete(options.outwardSuperDB.locations, Nat.compare, outwardKey);
    };

    type DeleteDBOptions = {superDB: SuperDB; outwardKey: OutwardSubDBKey};
    
    public func deleteSubDB(options: DeleteDBOptions) {
        switch(getInward({outwardSuperDB; outwardKey})) {
            case (?(inwardCanister, inwardKey)) {
                inwardCanister.deleteSubDBInward(inwardKey);
            };
            case (null) {};
        };
        options.outwardSuperDB.locations := BTree.delete(options.outwardSuperDB.locations, Nat.compare, outwardKey);
    };

    // Creating sub-DB //

    // It does not touch old items, so no locking.
    public func startCreatingSubDB({dbIndex: DBIndex; dbOptions: DBOptions; userData: Text}): async* Nat {
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
                        if (await part.isOverflowed({dbOptions})) { // TODO: Join .isOverflowed and .newCanister into one call?
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

    // TODO: here and in other places `inward` -> `inner` and `outward` -> `outer`
    type IterInwardOptions = {inwardSuperDB: SuperDB; inwardSubDBKey: InwardSubDBKey; dir: RBT.Direction};
    
    public func iterByInward(options: IterInwardOptions) : I.Iter<(Text, AttributeValue)> {
        switch (getSubDB(options.inwardSuperDB, options.inwardSubDBKey)) {
            case (?subDB) {
                RBT.iter(subDB.map, options.dir);
            };
            case (null) {
                Debug.trap("missing sub-DB");
            };
        };
    };

    // Impossible to implement.
    // type IterOutwardOptions = {outwardSuperDB: SuperDB; outwardSubDBKey: InwardSubDBKey; dir: RBT.Direction};
    
    // public func iterByOutward(options: IterOptions) : I.Iter<(Text, AttributeValue)> {
    // };

    type EntriesInwardOptions = {inwardSuperDB: SuperDB; inwardKey: InwardSubDBKey};
    
    public func entriesInward(options: EntriesOptions) : I.Iter<(Text, AttributeValue)> {
        iter({inwardSuperDB = options.inwardSuperDB; inwardKey = options.inwardKey; dir = #fwd});
    };

    // Impossible to implement.
    // type EntriesOutwardOptions = {outwardSuperDB: SuperDB; outwardKey: OutwardSubDBKey};
    
    // public func entriesOutward(options: EntriesOptions) : I.Iter<(Text, AttributeValue)> {
    // };

    type EntriesRevInwardOptions = {inwardSuperDB: SuperDB; inwardKey: InwardSubDBKey};
    
    public func entriesRev(options: EntriesOptions) : I.Iter<(Text, AttributeValue)> {
        iter({inwardSuperDB = options.inwardSuperDB; inwardKey = options.inwardKey; dir = #bwd});
    };

    // Impossible to implement.
    // type EntriesRevOutwardOptions = {outwardSuperDB: SuperDB; outwardKey: OutwardSubDBKey};
    
    // public func entriesRevOutward(options: EntriesOptions) : I.Iter<(Text, AttributeValue)> {
    // };

    public type ScanLimitResult = {
        results: [(Text, AttributeValue)];
        nextKey: ?Text;
    };

    type ScanLimitInwardOptions = {inwardSuperDB: SuperDB; inwardKey: InwardSubDBKey; lowerBound: Text; upperBound: Text; dir: RBT.Direction; limit: Nat};
    
    public func scanLimit(options: ScanLimitOptions): RBT.ScanLimitResult<Text, AttributeValue> {
        switch (getSubDB(options.inwardSuperDB, options.inwardKey)) {
            case (?subDB) {
                RBT.scanLimit(subDB.map, Text.compare, options.lowerBound, options.upperBound, options.dir, options.limit);
            };
            case (null) {
                Debug.trap("missing sub-DB");
            };
        };
    };
};