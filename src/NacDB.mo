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
    public type InnerSubDBKey = Nat;

    /// Constant (regarding moving a sub-DB to another canister) key mapped to `InnerSubDBKey`.
    public type OuterSubDBKey = Nat;

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
        subDBKey: OuterSubDBKey;
    };

    public type SuperDB = {
        var nextInnerKey: Nat;
        var nextOuterKey: Nat;
        subDBs: BTree.BTree<InnerSubDBKey, SubDB>;
        /// The canister and the `SubDBKey` of this `RBT.Tree` is constant,
        /// even when the sub-DB to which it points moves to a different canister.
        var locations: RBT.Tree<OuterSubDBKey, (PartitionCanister, InnerSubDBKey)>;

        var moving: ?{
            // outerSuperDB: SuperDB; // cannot be passed together with `oldInnerSuperDB`...
            outerCanister: PartitionCanister; // ... so, this instead.
            outerKey: OuterSubDBKey;
            oldInnerCanister: PartitionCanister;
            oldInnerSuperDB: SuperDB;
            oldInnerSubDBKey: InnerSubDBKey;
            var newInnerCanister: ?{ // null - not yet determined
                canister: PartitionCanister;
                var newSubDBKey: ?InnerSubDBKey; // null - not yet determined
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
            -> async {inner: (PartitionCanister, InnerSubDBKey); outer: (PartitionCanister, OuterSubDBKey)};
    };

    public type PartitionCanister = actor {
        rawInsertSubDB(map: RBT.Tree<SK, AttributeValue>, userData: Text, dbOptions: DBOptions) : async InnerSubDBKey;
        isOverflowed({dbOptions: DBOptions}) : async Bool;
        superDBSize: query () -> async Nat;
        deleteSubDB({subDBKey: OuterSubDBKey}) : async ();
        startInserting({subDBKey: OuterSubDBKey; sk: SK; value: AttributeValue}) : async SparseQueue.SparseQueueKey;
        finishInserting({dbOptions : DBOptions; index : IndexCanister; insertId : SparseQueue.SparseQueueKey})
            : async {inner: (PartitionCanister, InnerSubDBKey); outer: (PartitionCanister, OuterSubDBKey)};
        getByInner: query (options: {subDBKey: InnerSubDBKey; sk: SK}) -> async ?AttributeValue;
        hasByInner: query (options: {subDBKey: InnerSubDBKey; sk: SK}) -> async Bool;
        getByOuter: query (options: {subDBKey: OuterSubDBKey; sk: SK}) -> async ?AttributeValue;
        hasByOuter: query (options: {subDBKey: OuterSubDBKey; sk: SK}) -> async Bool;
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
            var nextInnerKey = 0;
            var nextOuterKey = 0;
            subDBs = BTree.init<InnerSubDBKey, SubDB>(null);
            var locations = RBT.init();
            var moving = null;
            var inserting = SparseQueue.init(100);
        }
    };

    public type DBOptions = {
        hardCap: ?Nat;
        moveCap: MoveCap;
    };

    /// The "real" returned value is `outer`, but `inner` can be used for caching
    /// (on cache failure retrieve new `inner` using `outer`).
    public func rawInsertSubDB(part: PartitionCanister, superDB: SuperDB, map: RBT.Tree<SK, AttributeValue>, userData: Text, dbOptions: DBOptions)
        : {outer: OuterSubDBKey; inner: InnerSubDBKey}
    {
        let inner = switch (superDB.moving) {
            case (?_) { Debug.trap("DB is scaling") }; // TODO: needed?
            case (null) {
                let key = superDB.nextInnerKey;
                superDB.nextInnerKey += 1;
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
        superDB.locations := RBT.put(superDB.locations, Nat.compare, superDB.nextOuterKey, (part, inner));
        let result = {outer = superDB.nextOuterKey; inner};
        superDB.nextOuterKey += 1;
        result;
    };

    public func getInner(superDB: SuperDB, outerKey: InnerSubDBKey) : ?(PartitionCanister, InnerSubDBKey) {
        RBT.get(superDB.locations, Nat.compare, outerKey);
    };

    public func getSubDBByInner(superDB: SuperDB, subDBKey: InnerSubDBKey) : ?SubDB {
        BTree.get(superDB.subDBs, Nat.compare, subDBKey);
    };

    public func putLocation(outerSuperDB: SuperDB, outerKey: OuterSubDBKey, innerCanister: PartitionCanister, innerKey: InnerSubDBKey) {
        outerSuperDB.locations := RBT.put(outerSuperDB.locations, Nat.compare, outerKey, (innerCanister, innerKey));
    };

    /// This function makes no sense, because it would return the entire sub-DB from another canister.
    // public func getSubDBByOuter(superDB: SuperDB, subDBKey: OuterSubDBKey) : ?SubDB {
    // };

    // FIXME: Linearize moving sub-DBs, with anybody having the right to finish a move.
    //        (Queue for index or for partition?)
    /// Moves to the specified `newCanister` or to a newly allocated canister, if null.
    func startMovingSubDBImpl({
        outerCanister: PartitionCanister; // ... so, this instead.
        outerKey: OuterSubDBKey;
        oldInnerCanister: PartitionCanister;
        oldInnerSuperDB: SuperDB;
        oldInnerSubDBKey: InnerSubDBKey;
        newCanister: ?PartitionCanister;
    }) {
        switch (oldInnerSuperDB.moving) { // FIXME: Should `moving` be here?
            case (?_) { Debug.trap("already moving") };
            case (null) {
                oldInnerSuperDB.moving := ?{
                    outerCanister;
                    outerKey;
                    oldInnerCanister;
                    oldInnerSuperDB;
                    oldInnerSubDBKey;
                    var newInnerCanister = do ? {
                        {
                            canister = newCanister!;
                            var newSubDBKey = null;
                        };
                    };
                };
            };
        };
    };

    // FIXME: arguments for inner/outer
    // FIXME: What to do with the case if the new sub-DB is already created and the old is not yet deleted? (Needs cleanup)
    public func finishMovingSubDB({index: IndexCanister; oldSuperDB: SuperDB; dbOptions: DBOptions}) : async* ?(PartitionCanister, InnerSubDBKey) {
        switch (oldSuperDB.moving) {
            case (?moving) {
                switch (BTree.get(moving.oldInnerSuperDB.subDBs, Nat.compare, moving.oldInnerSubDBKey)) {
                    case (?subDB) {
                        let (canister, newCanister) = switch (moving.newInnerCanister) {
                            case (?newCanister) { (newCanister.canister, newCanister) };
                            case (null) {
                                let newCanister = await index.newCanister();
                                let s = {canister = newCanister; var newSubDBKey: ?InnerSubDBKey = null};
                                moving.newInnerCanister := ?s;
                                (newCanister, s);
                            };
                        };
                        let newInnerSubDBKey = switch (newCanister.newSubDBKey) {
                            case (?newSubDBKey) { newSubDBKey };
                            case (null) {
                                let newSubDBKey = await canister.rawInsertSubDB(subDB.map, subDB.userData, dbOptions);
                                newCanister.newSubDBKey := ?newSubDBKey;
                                newSubDBKey;
                            }
                        };                        
                        ignore BTree.delete(oldSuperDB.subDBs, Nat.compare, moving.oldInnerSubDBKey); // FIXME: idempotent?
                        await outerCanister.putLocation(outerKey, moving.newInnerSubDBKey);
                        subDB.busy := false;
                        oldSuperDB.moving := null;
                        return ?(canister, newInnerSubDBKey); // TODO: need to return inner key?
                    };
                    case (null) {
                        return ?(moving.oldInnerCanister, moving.oldInnerSubDBKey);
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
        oldSubDBKey: InnerSubDBKey;
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
            oldInnerCanister: PartitionCanister;
            oldInnerSuperDB: SuperDB;
            oldInnerKey: InnerSubDBKey;
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

    public type GetByInnerOptions = {superDB: SuperDB; subDBKey: InnerSubDBKey; sk: SK};

    public func getByInner(options: GetByInnerOptions) : ?AttributeValue {
        switch (getSubDB(options.superDB, options.subDBKey)) {
            case (?subDB) {
                RBT.get(subDB.map, Text.compare, options.sk);
            };
            case (null) {
                Debug.trap("missing sub-DB")
            }
        }
    };

    public type GetByOuterOptions = {outerSuperDB: SuperDB; subDBKey: OuterSubDBKey; sk: SK};

    public func getByOuter(options: GetByOuterOptions) : async* ?AttributeValue {
        let ?(part, inner) = getInner(outerSuperDB, outerKey) else {
            Debug.trap("no entry");
        };
        await part.getByInner(inner);
    };

    public type ExistsByInnerOptions = GetByInnerOptions;

    public func hasByInner(options: ExistsByInnerOptions) : Bool {
        getByInner(options) != null;
    };

    public type ExistsByOuterOptions = GetByOuterOptions;

    public func hasByOuter(options: ExistsByOuterOptions) : Bool {
        getByOuter(options) != null;
    };

    public type HasDBByInnerOptions = {innerSuperDB: SuperDB; innerKey: InnerSubDBKey};

    public func hasSubDBByInner(options: HasDBByInnerOptions) : Bool {
        BTree.has(options.innerSuperDB.subDBs, Nat.compare, options.subDBKey);
    };

    public type HasDBByOuterOptions = {outerSuperDB: SuperDB; outerKey: OuterSubDBKey};

    public func hasSubDBByOuter(options: HasDBByOuterOptions) : Bool {
        let subDB = RBT.get(options.outerSuperDB, options.outerKey, Nat.compare);
        subDB != null and (do ? {
            BTree.has(subDB!, Nat.compare, options.subDBKey);
        });
    };

    // TODO: This inner and outer.
    // public type GetUserDataOptions = {superDB: SuperDB; subDBKey: SubDBKey};

    // // TODO: Test this function
    // public func getSubDBUserData(options: GetUserDataOptions) : ?Text {
    //     do ? { BTree.get(options.superDB.subDBs, Nat.compare, options.subDBKey)!.userData };
    // };

    public func superDBSize(superDB: SuperDB): Nat = BTree.size(superDB.subDBs);

    public type SubDBSizeByInnerOptions = {superDB: SuperDB; subDBKey: InnerSubDBKey};

    public func subDBSizeByInner(options: SubDBSizeByInnerOptions): ?Nat {
        do ? {
            ?RBT.size(getSubDB(options.superDB, options.subDBKey)!.map);
        }
    };

    public type SubDBSizeByOuterOptions = {superDB: SuperDB; subDBKey: OuterSubDBKey};

    public func subDBSizeByOuter(options: SubDBSizeByOuterOptions): ?Nat {
        let ?(part, innerKey) = RBT.get(options.outerSuperDB, options.outerKey, Nat.compare) else {
            Debug.trap("no sub-DB");
        };
        do ? {
            subDBSizeByInner(part, innerKey);
        };
    };

    /// To be called in a partition where `innerSuperDB` resides.
    public func startInsertingImpl(options: {
        dbOptions: DBOptions;
        indexCanister: IndexCanister;
        outerCanister: PartitionCanister;
        outerSuperDB: SuperDB;
        outerKey: OuterSubDBKey;
        sk: SK;
        value: AttributeValue;
        innerSuperDB: SuperDB;
        innerKey: InnerSubDBKey;
    }) : async* Nat {
        switch (getSubDBByInner(options.innerSuperDB, options.innerKey)) {
            case (?subDB) {
                subDB.map := RBT.put(subDB.map, Text.compare, options.sk, options.value);
                removeLoosers({subDB; dbOptions = options.dbOptions});

                let insertId = SparseQueue.add<InsertingItem>(options.innerSuperDB.inserting, { // FIXME: Is `inserting` at correct place?
                    part = options.outerCanister;
                    subDBKey = options.outerKey;
                });

                await* startMovingSubDBIfOverflow({
                    dbOptions = options.dbOptions;
                    index = options.indexCanister;
                    indexCanister = options.indexCanister;
                    oldInnerCanister = options.outerCanister;
                    oldInnerSuperDB = options.outerSuperDB;
                    oldInnerKey = options.innerKey;
                });

                insertId;
            };
            case (null) {
                Debug.trap("missing sub-DB");
            };
        };
    };

    public type InsertOptions = {
        dbOptions: DBOptions;
        indexCanister: IndexCanister;
        outerCanister: PartitionCanister;
        outerSuperDB: SuperDB;
        outerKey: OuterSubDBKey;
        sk: SK;
        value: AttributeValue;
    };

    /// There is no `startInsertingByInner`, because inserting may need to move the sub-DB.
    public func startInserting(options: InsertOptions) : async* Nat {
        let ?(innerCanister, innerKey) = getInner(options.outerSuperDB, options.outerKey) else {
            Debug.trap("missing sub-DB");
        };
        await innerCanister.startInsertingImpl({
            dbOptions = options.dbOptions;
            indexCanister = options.indexCanister;
            outerCanister = options.outerCanister;
            outerSuperDB = options.outerSuperDB;
            outerKey = options.outerKey;
            sk = options.sk;
            value = options.value;
            innerKey;
        });
    };

    public func finishInserting({index: IndexCanister; oldSuperDB: SuperDB; dbOptions: DBOptions; insertId: SparseQueue.SparseQueueKey})
        : async* {inner: (PartitionCanister, InnerSubDBKey); outer: (PartitionCanister, OuterSubDBKey)}
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

    type DeleteOptions = {outerSuperDB: SuperDB; outerKey: OuterSubDBKey; sk: SK};
    
    /// idempotent
    public func delete(options: DeleteOptions) {
        switch(getInner({outerSuperDB; outerKey})) {
            case (?(innerCanister, innerKey)) {
                innerCanister.deleteInner(innerKey, options.sk);
            };
            case (null) {};
        };
        options.outerSuperDB.locations := BTree.delete(options.outerSuperDB.locations, Nat.compare, outerKey);
    };

    type DeleteDBOptions = {superDB: SuperDB; outerKey: OuterSubDBKey};
    
    public func deleteSubDB(options: DeleteDBOptions) {
        switch(getInner({outerSuperDB; outerKey})) {
            case (?(innerCanister, innerKey)) {
                innerCanister.deleteSubDBInner(innerKey);
            };
            case (null) {};
        };
        options.outerSuperDB.locations := BTree.delete(options.outerSuperDB.locations, Nat.compare, outerKey);
    };

    // Creating sub-DB //

    // It does not touch old items, so no locking.
    public func startCreatingSubDB({dbIndex: DBIndex; dbOptions: DBOptions; userData: Text}): async* Nat {
        SparseQueue.add<CreatingSubDB>(dbIndex.creatingSubDB, {var canister = null; userData});
    };

    /// In the current version two partition canister are always the same.
    ///
    /// `superDB` should reside in `part`.
    func bothKeys(superDB: SuperDB, part: PartitionCanister, innerKey: InnerSubDBKey)
        : {inner: (PartitionCanister, InnerSubDBKey); outer: (PartitionCanister, OuterSubDBKey)}
    {
        superDB.locations := RBT.insert(superDB.locations, superDB.nextOuterKey, Nat.compare, (part, inner));
        let result = {inner: (part, inner); outer: (part, superDB.nextOuterKey)};
        superDB.nextOuterKey += 1;
        result;
    };

    /// The "real" returned value is `outer`, but `inner` can be used for caching
    /// (on cache failure retrieve new `inner` using `outer`).
    ///
    /// In this version returned `PartitionCanister` for inner and outer always the same.
    public func finishCreatingSubDB({index: IndexCanister; dbIndex: DBIndex; dbOptions: DBOptions; creatingId: Nat})
        : async* {inner: (PartitionCanister, InnerSubDBKey); outer: (PartitionCanister, OuterSubDBKey)}
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
                let innerKey = await part.rawInsertSubDB(RBT.init(), creating.userData, dbOptions); // We don't need `busy == true`, because we didn't yet created "links" to it.
                SparseQueue.delete(dbIndex.creatingSubDB, creatingId);
                bothKeys(part, innerKey);
            };
            case (null) {
                Debug.trap("not creating");
            };
        };
    };

    // Scanning/enumerating //

    // TODO: here and in other places `inner` -> `inner` and `outer` -> `outer`
    type IterInnerOptions = {innerSuperDB: SuperDB; innerSubDBKey: InnerSubDBKey; dir: RBT.Direction};
    
    public func iterByInner(options: IterInnerOptions) : I.Iter<(Text, AttributeValue)> {
        switch (getSubDB(options.innerSuperDB, options.innerSubDBKey)) {
            case (?subDB) {
                RBT.iter(subDB.map, options.dir);
            };
            case (null) {
                Debug.trap("missing sub-DB");
            };
        };
    };

    // Impossible to implement.
    // type IterOuterOptions = {outerSuperDB: SuperDB; outerSubDBKey: InnerSubDBKey; dir: RBT.Direction};
    
    // public func iterByOuter(options: IterOptions) : I.Iter<(Text, AttributeValue)> {
    // };

    type EntriesInnerOptions = {innerSuperDB: SuperDB; innerKey: InnerSubDBKey};
    
    public func entriesInner(options: EntriesInnerOptions) : I.Iter<(Text, AttributeValue)> {
        iter({innerSuperDB = options.innerSuperDB; innerKey = options.innerKey; dir = #fwd});
    };

    // Impossible to implement.
    // type EntriesOuterOptions = {outerSuperDB: SuperDB; outerKey: OuterSubDBKey};
    
    // public func entriesOuter(options: EntriesOptions) : I.Iter<(Text, AttributeValue)> {
    // };

    type EntriesRevInnerOptions = {innerSuperDB: SuperDB; innerKey: InnerSubDBKey};
    
    public func entriesRev(options: EntriesRevInnerOptions) : I.Iter<(Text, AttributeValue)> {
        iter({innerSuperDB = options.innerSuperDB; innerKey = options.innerKey; dir = #bwd});
    };

    // Impossible to implement.
    // type EntriesRevOuterOptions = {outerSuperDB: SuperDB; outerKey: OuterSubDBKey};
    
    // public func entriesRevOuter(options: EntriesOptions) : I.Iter<(Text, AttributeValue)> {
    // };

    public type ScanLimitResult = {
        results: [(Text, AttributeValue)];
        nextKey: ?Text;
    };

    type ScanLimitInnerOptions = {innerSuperDB: SuperDB; innerKey: InnerSubDBKey; lowerBound: Text; upperBound: Text; dir: RBT.Direction; limit: Nat};
    
    public func scanLimitInner(options: ScanLimitInnerOptions): RBT.ScanLimitResult<Text, AttributeValue> {
        switch (getSubDB(options.innerSuperDB, options.innerKey)) {
            case (?subDB) {
                RBT.scanLimit(subDB.map, Text.compare, options.lowerBound, options.upperBound, options.dir, options.limit);
            };
            case (null) {
                Debug.trap("missing sub-DB");
            };
        };
    };

    type ScanLimitOuterOptions = {outerSuperDB: SuperDB; outerKey: OuterSubDBKey; lowerBound: Text; upperBound: Text; dir: RBT.Direction; limit: Nat};
    
    public func scanLimitOuter(options: ScanLimitOuterOptions): RBT.ScanLimitResult<Text, AttributeValue> {
        let ?(part, innerKey) = getInner(outerSuperDB, outerKey) else {
            Debug.trap("no sub-DB");
        };
        part.scanLimitInner({innerKey; lowerBound = options.lowerBound; upperBound = options.upperBound; dir = options.dir; limit = options.limit});
    };
};