import Cycles "mo:base/ExperimentalCycles";
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
import Blob "mo:base/Blob";

module {
    public type GUID = Blob;

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
        // TODO: Express dependencies not as Bools but as enums.
        var insertingImplDone: Bool;
        var finishMovingSubDBDone: ?{
            // new ones (TODO: name with the word "new")
            innerPartition: PartitionCanister;
            innerKey: OuterSubDBKey;
        };
        // old: ?{
        //     partitionCanister: PartitionCanister;
        //     innerKey: OuterSubDBKey;
        // };
    };

    public type InsertingItem2 = {
        var newInnerCanister: ?{
            canister: PartitionCanister;
            var innerKey: ?InnerSubDBKey;
        };
    };

    public type SuperDB = {
        var nextInnerKey: Nat;
        var nextOuterKey: Nat;
        subDBs: BTree.BTree<InnerSubDBKey, SubDB>;
        /// The canister and the `SubDBKey` of this `RBT.Tree` is constant,
        /// even when the sub-DB to which it points moves to a different canister.
        var locations: RBT.Tree<OuterSubDBKey, (PartitionCanister, InnerSubDBKey)>;

        // TODO: Which variables can be removed from `moving` and `moving2`?
        var moving: ?{
            // outerSuperDB: SuperDB; // cannot be passed together with `oldInnerSuperDB`...
            outerCanister: PartitionCanister; // ... so, this instead.
            outerKey: OuterSubDBKey;
            oldInnerCanister: PartitionCanister;
            oldInnerSuperDB: SuperDB;
            oldInnerSubDBKey: InnerSubDBKey;
        };
        var moving2: ?{ // TODO: Initialize this when start moving.
            outerCanister: PartitionCanister;
            outerKey: OuterSubDBKey;
            oldSuperDB: SuperDB;
            oldInnerKey: InnerSubDBKey;
            var newInnerCanister: ?{ // null - not yet determined
                var newInnerCanister: PartitionCanister;
                var newInnerKey: ?InnerSubDBKey; // null - not yet determined
            };
        };
        var inserting: SparseQueue.SparseQueue<InsertingItem>;  // outer
        var inserting2: SparseQueue.SparseQueue<InsertingItem2>; // inner
    };

    public type DBIndex = {
        var canisters: StableBuffer.StableBuffer<PartitionCanister>;
        var creatingSubDB: SparseQueue.SparseQueue<CreatingSubDB>;
    };

    public type IndexCanister = actor {
        getCanisters: query () -> async [PartitionCanister];
        newCanister(): async PartitionCanister;
        createSubDB: shared({dbOptions: DBOptions; userData: Text})
            -> async {inner: (PartitionCanister, InnerSubDBKey); outer: (PartitionCanister, OuterSubDBKey)};
    };

    // TODO: Can we have separate type for inner and outer canisters?
    // TODO: arguments as {...}, not (...).
    public type PartitionCanister = actor {
        // TODO: Remove superfluous, if any.
        rawInsertSubDB(map: RBT.Tree<SK, AttributeValue>, userData: Text, dbOptions: DBOptions) : async InnerSubDBKey;
        isOverflowed({dbOptions: DBOptions}) : async Bool;
        superDBSize: query () -> async Nat;
        deleteSubDB({subDBKey: OuterSubDBKey}) : async ();
        deleteSubDBInner(innerKey: InnerSubDBKey) : async ();
        finishMovingSubDBImpl({
            guid: GUID;
            index: IndexCanister;
            outerCanister: PartitionCanister;
            outerKey: OuterSubDBKey;
            oldInnerKey: InnerSubDBKey;
            dbOptions: DBOptions;
        }) : async (PartitionCanister, InnerSubDBKey);
        insert({
            guid: GUID;
            dbOptions: DBOptions;
            indexCanister: IndexCanister;
            outerCanister: PartitionCanister;
            outerKey: OuterSubDBKey;
            sk: SK;
            value: AttributeValue;
        }) : async ();
        putLocation(outerKey: OuterSubDBKey, innerCanister: PartitionCanister, newInnerSubDBKey: InnerSubDBKey) : async ();
        createOuter(part: PartitionCanister, innerKey: InnerSubDBKey)
            : async {inner: (PartitionCanister, InnerSubDBKey); outer: (PartitionCanister, OuterSubDBKey)};
        deleteInner(innerKey: InnerSubDBKey, sk: SK): async ();
        scanLimitInner({innerKey: InnerSubDBKey; lowerBound: SK; upperBound: SK; dir: RBT.Direction; limit: Nat})
            : async RBT.ScanLimitResult<Text, AttributeValue>;
        getByInner: query (options: {subDBKey: InnerSubDBKey; sk: SK}) -> async ?AttributeValue;
        hasByInner: query (options: {subDBKey: InnerSubDBKey; sk: SK}) -> async Bool;
        getByOuter: query (options: {subDBKey: OuterSubDBKey; sk: SK}) -> async ?AttributeValue;
        hasByOuter: query (options: {subDBKey: OuterSubDBKey; sk: SK}) -> async Bool;
        hasSubDBByInner: query (options: {subDBKey: InnerSubDBKey}) -> async Bool;
        subDBSizeByInner: query (options: {subDBKey: InnerSubDBKey}) -> async ?Nat;
        startInsertingImpl: query (options: {
            dbOptions: DBOptions;
            indexCanister: IndexCanister;
            outerCanister: PartitionCanister;
            outerKey: OuterSubDBKey;
            sk: SK;
            value: AttributeValue;
            innerKey: InnerSubDBKey;
        }) -> async ();
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
            var moving2 = null;
            var inserting = SparseQueue.init(100);
            var inserting2 = SparseQueue.init(100);
        }
    };

    public type DBOptions = {
        hardCap: ?Nat;
        moveCap: MoveCap;
        constructor: shared(dbOptions: DBOptions) -> async PartitionCanister;
        newPartitionCycles: Nat;
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

    /// Moves to the specified `newCanister` or to a newly allocated canister, if null.
    ///
    /// This is meant to be called without checking user identity.
    func startMovingSubDBImpl({
        outerCanister: PartitionCanister;
        outerKey: OuterSubDBKey;
        oldInnerCanister: PartitionCanister;
        oldInnerSuperDB: SuperDB;
        oldInnerSubDBKey: InnerSubDBKey;
        newCanister: ?PartitionCanister;
    }) {
        switch (oldInnerSuperDB.moving) { // FIXME: `moving` belongs to outer super-DB (here and in other places).
            case (?_) { Debug.trap("already moving") };
            case (null) {
                oldInnerSuperDB.moving := ?{
                    outerCanister;
                    outerKey;
                    oldInnerCanister;
                    oldInnerSuperDB;
                    oldInnerSubDBKey;
                    // var newInnerCanister = do ? {
                    //     {
                    //         canister = newCanister!;
                    //         var newSubDBKey = null;
                    //     };
                    // };
                };
            };
        };
    };

    func finishMovingSubDBImpl({
        guid: GUID;
        index: IndexCanister;
        outerCanister: PartitionCanister;
        outerKey: OuterSubDBKey;
        oldInnerSuperDB: SuperDB;
        oldInnerKey: InnerSubDBKey;
        dbOptions: DBOptions
    }) : async* (PartitionCanister, InnerSubDBKey)
    {
        let inserting2 = SparseQueue.add<InsertingItem2>(oldInnerSuperDB.inserting2, guid, {
            var newInnerCanister = null;
        });
        
        switch (BTree.get(oldInnerSuperDB.subDBs, Nat.compare, oldInnerKey)) {
            case (?subDB) {
                let (canister, newCanister) = switch (inserting2.newInnerCanister) {
                    case (?newCanister) { (newCanister.canister, newCanister) };
                    case (null) {
                        let newCanister = await index.newCanister();
                        let s = {canister = newCanister; var innerKey: ?InnerSubDBKey = null};
                        inserting2.newInnerCanister := ?s;
                        (newCanister, s);
                    };
                };
                let newInnerSubDBKey = switch (newCanister.innerKey) {
                    case (?newSubDBKey) { newSubDBKey };
                    case (null) {
                        let newSubDBKey = await canister.rawInsertSubDB(subDB.map, subDB.userData, dbOptions);
                        newCanister.innerKey := ?newSubDBKey;
                        newSubDBKey;
                    }
                };                        
                await outerCanister.putLocation(outerKey, canister, newInnerSubDBKey);
                ignore BTree.delete(oldInnerSuperDB.subDBs, Nat.compare, oldInnerKey); // FIXME: idempotent?
                subDB.busy := false; // FIXME
                (canister, newInnerSubDBKey);
            };
            case (null) {
                Debug.trap("no sub-DB");
            };
        };
    };

    func startMovingSubDB(options: {
        dbOptions: DBOptions;
        index: IndexCanister;
        outerCanister: PartitionCanister;
        outerKey: OuterSubDBKey;
        oldCanister: PartitionCanister;
        oldInnerSuperDB: SuperDB;
        oldInnerSubDBKey: InnerSubDBKey; // TODO: redundant (or preserve for efficiency?)
    }) : async* () {
        let ?item = BTree.get(options.oldInnerSuperDB.subDBs, Nat.compare, options.oldInnerSubDBKey) else {
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
                outerCanister = options.outerCanister;
                outerKey = options.outerKey;
                oldInnerCanister = options.oldCanister;
                oldInnerSuperDB = options.oldInnerSuperDB;
                oldInnerSubDBKey = options.oldInnerSubDBKey;
                newCanister = null;
            });
        } else {
            startMovingSubDBImpl({
                outerCanister = options.outerCanister;
                outerKey = options.outerKey;
                oldInnerCanister = options.oldCanister;
                oldInnerSuperDB = options.oldInnerSuperDB;
                oldInnerSubDBKey = options.oldInnerSubDBKey;
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
            outerCanister: PartitionCanister;
            outerKey: OuterSubDBKey;
            oldInnerCanister: PartitionCanister;
            oldInnerSuperDB: SuperDB;
            oldInnerKey: InnerSubDBKey;
        }): async* ()
    {
        if (await options.oldInnerCanister.isOverflowed({dbOptions = options.dbOptions})) {
            await* startMovingSubDB({
                dbOptions = options.dbOptions;
                index = options.index;
                outerCanister = options.outerCanister;
                outerKey = options.outerKey;
                oldCanister = options.oldInnerCanister;
                oldInnerSuperDB = options.oldInnerSuperDB;
                oldInnerSubDBKey = options.oldInnerKey;
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
        switch (getSubDBByInner(options.superDB, options.subDBKey)) {
            case (?subDB) {
                RBT.get(subDB.map, Text.compare, options.sk);
            };
            case (null) {
                Debug.trap("missing sub-DB")
            }
        }
    };

    public type GetByOuterOptions = {outerSuperDB: SuperDB; outerKey: OuterSubDBKey; sk: SK};

    public func getByOuter(options: GetByOuterOptions) : async* ?AttributeValue {
        let ?(part, inner) = getInner(options.outerSuperDB, options.outerKey) else {
            Debug.trap("no entry");
        };
        await part.getByInner({subDBKey = inner; sk = options.sk});
    };

    public type ExistsByInnerOptions = GetByInnerOptions;

    public func hasByInner(options: ExistsByInnerOptions) : Bool {
        getByInner(options) != null;
    };

    public type ExistsByOuterOptions = GetByOuterOptions;

    public func hasByOuter(options: ExistsByOuterOptions) : async* Bool {
        (await* getByOuter(options)) != null;
    };

    public type HasDBByInnerOptions = {innerSuperDB: SuperDB; innerKey: InnerSubDBKey};

    public func hasSubDBByInner(options: HasDBByInnerOptions) : Bool {
        BTree.has(options.innerSuperDB.subDBs, Nat.compare, options.innerKey);
    };

    public type HasDBByOuterOptions = {outerSuperDB: SuperDB; outerKey: OuterSubDBKey};

    public func hasSubDBByOuter(options: HasDBByOuterOptions) : async* Bool {
        let ?(part, inner) = getInner(options.outerSuperDB, options.outerKey) else {
            Debug.trap("no entry");
        };
        await part.hasSubDBByInner({subDBKey = inner});
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
            RBT.size(getSubDBByInner(options.superDB, options.subDBKey)!.map);
        }
    };

    public type SubDBSizeByOuterOptions = {outerSuperDB: SuperDB; outerKey: OuterSubDBKey};

    public func subDBSizeByOuter(options: SubDBSizeByOuterOptions): async ?Nat {
        let ?(part, innerKey) = getInner(options.outerSuperDB, options.outerKey) else {
            Debug.trap("no sub-DB");
        };
        await part.subDBSizeByInner({subDBKey = innerKey});
    };

    /// To be called in a partition where `innerSuperDB` resides.
    public func startInsertingImpl(options: {
        guid: GUID;
        dbOptions: DBOptions;
        indexCanister: IndexCanister;
        outerCanister: PartitionCanister;
        outerSuperDB: SuperDB;
        outerKey: OuterSubDBKey;
        sk: SK;
        value: AttributeValue;
        innerSuperDB: SuperDB;
        innerKey: InnerSubDBKey;
    }) : async () {
        switch (getSubDBByInner(options.innerSuperDB, options.innerKey)) {
            case (?subDB) {
                subDB.map := RBT.put(subDB.map, Text.compare, options.sk, options.value);
                removeLoosers({subDB; dbOptions = options.dbOptions});

                await* startMovingSubDBIfOverflow({
                    dbOptions = options.dbOptions;
                    index = options.indexCanister;
                    outerCanister = options.outerCanister;
                    outerKey = options.outerKey;
                    indexCanister = options.indexCanister;
                    oldInnerCanister = options.outerCanister;
                    oldInnerSuperDB = options.outerSuperDB;
                    oldInnerKey = options.innerKey;
                });
            };
            case (null) {
                Debug.trap("missing sub-DB");
            };
        };
    };

    public type InsertOptions = {
        guid: GUID;
        dbOptions: DBOptions;
        indexCanister: IndexCanister;
        outerCanister: PartitionCanister;
        outerSuperDB: SuperDB;
        outerKey: OuterSubDBKey;
        sk: SK;
        value: AttributeValue;
    };

    /// There is no `insertByInner`, because inserting may need to move the sub-DB.
    public func insert(options: InsertOptions)
        : async* {inner: (PartitionCanister, InnerSubDBKey); outer: (PartitionCanister, OuterSubDBKey)}
    {
        let ?(oldInnerCanister, oldInnerKey) = getInner(options.outerSuperDB, options.outerKey) else {
            Debug.trap("missing sub-DB");
        };

        let inserting = SparseQueue.add<InsertingItem>(options.outerSuperDB.inserting, options.guid, {
            part = options.outerCanister;
            subDBKey = options.outerKey;
            var insertingImplDone = false;
            var finishMovingSubDBDone = null;
        });

        if (not inserting.insertingImplDone) {
            await oldInnerCanister.startInsertingImpl({
                guid = options.guid;
                dbOptions = options.dbOptions;
                indexCanister = options.indexCanister;
                outerCanister = options.outerCanister;
                outerKey = options.outerKey;
                sk = options.sk;
                value = options.value;
                innerKey = oldInnerKey;
            });
            inserting.insertingImplDone := true;
        };

        // TODO: check `.moving`
        let (newInnerPartition, newInnerKey) = switch (inserting.finishMovingSubDBDone) {
            case (?{innerPartition; innerKey}) { (innerPartition, innerKey) };
            case (null) {
                let (innerPartition, innerKey) = await oldInnerCanister.finishMovingSubDBImpl({
                    guid = options.guid; index = options.indexCanister; dbOptions = options.dbOptions;
                    oldInnerKey;
                    outerCanister = options.outerCanister;
                    outerKey = options.outerKey;
                });
                options.outerSuperDB.moving := null; // FIXME
                (innerPartition, innerKey)
            };
        };
        inserting.finishMovingSubDBDone := ?{
            innerPartition = newInnerPartition;
            innerKey = newInnerKey;
        };

        {inner = (newInnerPartition, newInnerKey); outer = (options.outerCanister, options.outerKey)};
    };

    type DeleteOptions = {outerSuperDB: SuperDB; outerKey: OuterSubDBKey; sk: SK};
    
    /// idempotent
    public func delete(options: DeleteOptions): async* () {
        switch(getInner(options.outerSuperDB, options.outerKey)) {
            case (?(innerCanister, innerKey)) {
                await innerCanister.deleteInner(innerKey, options.sk);
            };
            case (null) {};
        };
        options.outerSuperDB.locations := RBT.delete(options.outerSuperDB.locations, Nat.compare, options.outerKey);
    };

    type DeleteDBOptions = {outerSuperDB: SuperDB; outerKey: OuterSubDBKey};
    
    public func deleteSubDB(options: DeleteDBOptions): async* () {
        switch(getInner(options.outerSuperDB, options.outerKey)) {
            case (?(innerCanister, innerKey)) {
                await innerCanister.deleteSubDBInner(innerKey);
            };
            case (null) {};
        };
        options.outerSuperDB.locations := RBT.delete(options.outerSuperDB.locations, Nat.compare, options.outerKey);
    };

    // Creating sub-DB //

    /// It does not touch old items, so no locking.
    ///
    /// Pass a random GUID. Repeat the call with the same GUID, if the previous call failed.
    ///
    /// The "real" returned value is `outer`, but `inner` can be used for caching
    /// (on cache failure retrieve new `inner` using `outer`).
    ///
    /// In this version returned `PartitionCanister` for inner and outer always the same.
    public func createSubDB({guid: GUID; dbIndex: DBIndex; dbOptions: DBOptions; userData: Text})
        : async* {inner: (PartitionCanister, InnerSubDBKey); outer: (PartitionCanister, OuterSubDBKey)}
    {
        let creating0: CreatingSubDB = {var canister = null; userData};
        let creating = SparseQueue.add(dbIndex.creatingSubDB, guid, creating0);
        let part3: PartitionCanister = switch (creating.canister) {
            case (?part) { part };
            case (null) {
                let canisters = StableBuffer.toArray(dbIndex.canisters); // TODO: a special function for this
                let part = canisters[canisters.size() - 1];
                let part2 = if (await part.isOverflowed({dbOptions})) { // TODO: Join .isOverflowed and .newCanister into one call?
                    let part2 = await* newCanister(dbOptions, dbIndex);
                    creating.canister := ?part;
                    part2;
                } else {
                    let innerKey = await part.rawInsertSubDB(RBT.init(), creating.userData, dbOptions); // We don't need `busy == true`, because we didn't yet created "links" to it.
                    // SparseQueue.delete(dbIndex.creatingSubDB, creatingId); // FIXME: Avoid calling `rawInsertSubDB` repeatedly (idempotency).
                    return await part.createOuter(part, innerKey);
                };
                part2;
            };
        };
        let innerKey = await part3.rawInsertSubDB(RBT.init(), creating.userData, dbOptions); // We don't need `busy == true`, because we didn't yet created "links" to it.
        // SparseQueue.delete(dbIndex.creatingSubDB, creatingId); // FIXME: Ensure idempotency.
        await part3.createOuter(part3, innerKey); // FIXME: outer part vs inner part? (and need to do external call of this function here and in other places?)
    };

    /// In the current version two partition canister are always the same.
    ///
    /// `superDB` should reside in `part`.
    func createOuter(outerSuperDB: SuperDB, part: PartitionCanister, innerKey: InnerSubDBKey)
        : {inner: (PartitionCanister, InnerSubDBKey); outer: (PartitionCanister, OuterSubDBKey)}
    {
        outerSuperDB.locations := RBT.put(outerSuperDB.locations, Nat.compare, outerSuperDB.nextOuterKey, (part, innerKey));
        let result = {inner = (part, innerKey); outer = (part, outerSuperDB.nextOuterKey)};
        outerSuperDB.nextOuterKey += 1;
        result;
    };

    // Scanning/enumerating //

    // TODO: here and in other places `inner` -> `inner` and `outer` -> `outer`
    type IterInnerOptions = {innerSuperDB: SuperDB; innerKey: InnerSubDBKey; dir: RBT.Direction};
    
    public func iterByInner(options: IterInnerOptions) : I.Iter<(Text, AttributeValue)> {
        switch (getSubDBByInner(options.innerSuperDB, options.innerKey)) {
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
        iterByInner({innerSuperDB = options.innerSuperDB; innerKey = options.innerKey; dir = #fwd});
    };

    // Impossible to implement.
    // type EntriesOuterOptions = {outerSuperDB: SuperDB; outerKey: OuterSubDBKey};
    
    // public func entriesOuter(options: EntriesOptions) : I.Iter<(Text, AttributeValue)> {
    // };

    type EntriesRevInnerOptions = {innerSuperDB: SuperDB; innerKey: InnerSubDBKey};
    
    public func entriesRev(options: EntriesRevInnerOptions) : I.Iter<(Text, AttributeValue)> {
        iterByInner({innerSuperDB = options.innerSuperDB; innerKey = options.innerKey; dir = #bwd});
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
        switch (getSubDBByInner(options.innerSuperDB, options.innerKey)) {
            case (?subDB) {
                RBT.scanLimit(subDB.map, Text.compare, options.lowerBound, options.upperBound, options.dir, options.limit);
            };
            case (null) {
                Debug.trap("missing sub-DB");
            };
        };
    };

    type ScanLimitOuterOptions = {outerSuperDB: SuperDB; outerKey: OuterSubDBKey; lowerBound: Text; upperBound: Text; dir: RBT.Direction; limit: Nat};
    
    public func scanLimitOuter(options: ScanLimitOuterOptions): async* RBT.ScanLimitResult<Text, AttributeValue> {
        let ?(part, innerKey) = getInner(options.outerSuperDB, options.outerKey) else {
            Debug.trap("no sub-DB");
        };
        await part.scanLimitInner({innerKey; lowerBound = options.lowerBound; upperBound = options.upperBound; dir = options.dir; limit = options.limit});
    };

    /// Canisters

    public func getCanisters(dbIndex: DBIndex): [PartitionCanister] {
        StableBuffer.toArray(dbIndex.canisters);
    };

    public func newCanister(dbOptions: DBOptions, dbIndex: DBIndex): async* PartitionCanister {
        Cycles.add(dbOptions.newPartitionCycles);
        let canister = await dbOptions.constructor(dbOptions);
        StableBuffer.add(dbIndex.canisters, canister); // TODO: too low level
        canister;
    };


};