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
import MyCycles "../lib/Cycles";
import Blob "mo:base/Blob";
import Array "mo:base/Array";
import Time "mo:base/Time";

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
    };

    public type MoveCap = { #usedMemory: Nat };

    public type CreatingSubDB = {
        var canister: ?PartitionCanister; // Immediately after creation of sub-DB, this is both inner and outer.
        var loc: ?{inner: (PartitionCanister, InnerSubDBKey); outer: (PartitionCanister, InnerSubDBKey)};
        userData: Text;
    };

    /// Treat this as an opaque data structure, because this data is ignored if the sub-DB moves during insertion.
    public type InsertingItem = {
        part: PartitionCanister; // TODO: Can we remove this?
        subDBKey: OuterSubDBKey;
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
            var innerKey: ?{ // TODO: Simplify the structure.
                key: InnerSubDBKey;
            }
        };
    };

    public type SuperDB = {
        var nextInnerKey: Nat;
        var nextOuterKey: Nat;
        subDBs: BTree.BTree<InnerSubDBKey, SubDB>;
        /// The canister and the `SubDBKey` of this `RBT.Tree` is constant,
        /// even when the sub-DB to which it points moves to a different canister.
        // TODO: Join the following two variables into one:
        var locations: BTree.BTree<OuterSubDBKey, (PartitionCanister, InnerSubDBKey)>;
        var busy: BTree.BTree<OuterSubDBKey, SparseQueue.GUID>; // TODO: Improve efficiency.

        // TODO: Which variables can be removed from `moving`?
        var moving: ?{
            // outerSuperDB: SuperDB; // cannot be passed together with `oldInnerSuperDB`...
            outerCanister: PartitionCanister; // ... so, this instead.
            outerKey: OuterSubDBKey;
            oldInnerCanister: PartitionCanister;
            oldInnerSuperDB: SuperDB;
            oldInnerSubDBKey: InnerSubDBKey;
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
        createSubDB: shared({guid: GUID; dbOptions: DBOptions; userData: Text})
            -> async {inner: (PartitionCanister, InnerSubDBKey); outer: (PartitionCanister, OuterSubDBKey)};
    };

    // TODO: Can we have separate type for inner and outer canisters?
    // TODO: arguments as {...}, not (...).
    public type PartitionCanister = actor {
        // TODO: Remove superfluous, if any.
        rawInsertSubDB(map: RBT.Tree<SK, AttributeValue>, inner: ?InnerSubDBKey, userData: Text, dbOptions: DBOptions)
            : async {inner: OuterSubDBKey};
        rawInsertSubDBAndSetOuter(
            canister: PartitionCanister,
            map: RBT.Tree<SK, AttributeValue>,
            keys: ?{
                inner: InnerSubDBKey;
                outer: OuterSubDBKey;
            },
            userData: Text,
            dbOptions: DBOptions
        )
            : async {inner: InnerSubDBKey; outer: OuterSubDBKey};
        isOverflowed: shared ({dbOptions: DBOptions}) -> async Bool;
        superDBSize: query () -> async Nat;
        deleteSubDB({outerKey: OuterSubDBKey; guid: GUID}) : async ();
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
        }) : async {inner: (PartitionCanister, InnerSubDBKey); outer: (PartitionCanister, OuterSubDBKey)};
        putLocation(outerKey: OuterSubDBKey, innerCanister: PartitionCanister, newInnerSubDBKey: InnerSubDBKey) : async ();
        createOuter(part: PartitionCanister, outerKey: OuterSubDBKey, innerKey: InnerSubDBKey)
            : async {inner: (PartitionCanister, InnerSubDBKey); outer: (PartitionCanister, OuterSubDBKey)};
        delete({outerKey: OuterSubDBKey; sk: SK; guid: GUID}): async ();
        deleteInner(innerKey: InnerSubDBKey, sk: SK): async ();
        scanLimitInner: query({innerKey: InnerSubDBKey; lowerBound: SK; upperBound: SK; dir: RBT.Direction; limit: Nat})
            -> async RBT.ScanLimitResult<Text, AttributeValue>;
        scanLimitOuter: shared({outerKey: OuterSubDBKey; lowerBound: SK; upperBound: SK; dir: RBT.Direction; limit: Nat})
            -> async RBT.ScanLimitResult<Text, AttributeValue>;
        getByInner: query (options: {subDBKey: InnerSubDBKey; sk: SK}) -> async ?AttributeValue;
        hasByInner: query (options: {subDBKey: InnerSubDBKey; sk: SK}) -> async Bool;
        getByOuter: shared (options: {subDBKey: OuterSubDBKey; sk: SK}) -> async ?AttributeValue;
        hasByOuter: shared (options: {subDBKey: OuterSubDBKey; sk: SK}) -> async Bool;
        hasSubDBByInner: query (options: {subDBKey: InnerSubDBKey}) -> async Bool;
        hasSubDBByOuter: shared (options: {subDBKey: OuterSubDBKey}) -> async Bool;
        subDBSizeByInner: query (options: {subDBKey: InnerSubDBKey}) -> async ?Nat;
        subDBSizeByOuter: shared (options: {subDBKey: OuterSubDBKey}) -> async ?Nat;
        startInsertingImpl(options: {
            guid: GUID;
            dbOptions: DBOptions;
            indexCanister: IndexCanister;
            outerCanister: PartitionCanister;
            outerKey: OuterSubDBKey;
            sk: SK;
            value: AttributeValue;
            innerKey: InnerSubDBKey;
        }): async ();
        scanSubDBs: query() -> async [(OuterSubDBKey, (PartitionCanister, InnerSubDBKey))];
    };

    public func createDBIndex(dbOptions: DBOptions) : DBIndex {
        {
            var canisters = StableBuffer.init<PartitionCanister>();
            var creatingSubDB = SparseQueue.init(dbOptions.createDBQueueLength, dbOptions.timeout);
            moveCap = dbOptions.moveCap;
        };
    };

    public func createSuperDB(dbOptions: DBOptions) : SuperDB {
        {
            var nextInnerKey = 0;
            var nextOuterKey = 0;
            subDBs = BTree.init<InnerSubDBKey, SubDB>(null);
            var locations = BTree.init(null);
            var busy = BTree.init(null);
            var moving = null;
            var inserting = SparseQueue.init(dbOptions.insertQueueLength, dbOptions.timeout);
            var inserting2 = SparseQueue.init(dbOptions.insertQueueLength, dbOptions.timeout);
        };
    };

    public type DBOptions = {
        hardCap: ?Nat;
        moveCap: MoveCap;
        constructor: shared(dbOptions: DBOptions) -> async PartitionCanister;
        partitionCycles: Nat;
        timeout: Time.Time;
        createDBQueueLength: Nat;
        insertQueueLength: Nat;
    };

    /// The "real" returned value is `outer`, but `inner` can be used for caching
    /// (on cache failure retrieve new `inner` using `outer`).
    public func rawInsertSubDB(
        superDB: SuperDB,
        innerCanister: PartitionCanister,
        map: RBT.Tree<SK, AttributeValue>,
        inner: ?InnerSubDBKey,
        userData: Text,
        dbOptions: DBOptions,
    ) : {inner: InnerSubDBKey}
    {
        let inner2 = switch (superDB.moving) {
            case (?_) { Debug.trap("DB is scaling") }; // TODO: needed?
            case (null) {
                let key = switch (inner) {
                    case (?key) { key };
                    case (null) {
                        let key = superDB.nextInnerKey;
                        superDB.nextInnerKey += 1;
                        key;
                    };
                };                    
                let subDB : SubDB = {
                    var map = map;
                    var userData = userData;
                };
                ignore BTree.insert(superDB.subDBs, Nat.compare, key, subDB);
                key;
            };
        };
        {inner = inner2};
    };

    /// Use only if sure that outer and inner canisters coincide.
    public func rawInsertSubDBAndSetOuter(
        superDB: SuperDB,
        canister: PartitionCanister,
        map: RBT.Tree<SK, AttributeValue>,
        keys: ?{
            inner: InnerSubDBKey;
            outer: OuterSubDBKey;
        },
        userData: Text,
        dbOptions: DBOptions,
    ) : {outer: OuterSubDBKey; inner: InnerSubDBKey}
    {
        let {inner = inner2} = rawInsertSubDB(superDB, canister, map, do ? {keys!.inner}, userData, dbOptions);
        if (keys == null) {
            ignore BTree.insert(superDB.locations, Nat.compare, superDB.nextOuterKey, (canister, inner2));
        };
        switch (keys) {
            case (?{inner; outer}) {
                {outer; inner};
            };
            case (null) {
                let result = {outer = superDB.nextOuterKey; inner = inner2; };
                superDB.nextOuterKey += 1;
                result;
            };
        };
    };

    public func getInner(superDB: SuperDB, outerKey: InnerSubDBKey) : ?(PartitionCanister, InnerSubDBKey) {
        BTree.get(superDB.locations, Nat.compare, outerKey);
    };

    public func getSubDBByInner(superDB: SuperDB, subDBKey: InnerSubDBKey) : ?SubDB {
        BTree.get(superDB.subDBs, Nat.compare, subDBKey);
    };

    public func putLocation(outerSuperDB: SuperDB, outerKey: OuterSubDBKey, innerCanister: PartitionCanister, innerKey: InnerSubDBKey) {
        ignore BTree.insert(outerSuperDB.locations, Nat.compare, outerKey, (innerCanister, innerKey));
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
        switch (oldInnerSuperDB.moving) {
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

    /// Called only if `isOverflowed`.
    public func finishMovingSubDBImpl({
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
        
        let result = switch (BTree.get(oldInnerSuperDB.subDBs, Nat.compare, oldInnerKey)) {
            case (?subDB) {
                let (canister, newCanister) = switch (inserting2.newInnerCanister) {
                    case (?newCanister) { (newCanister.canister, newCanister) };
                    case (null) {
                        MyCycles.addPart(dbOptions.partitionCycles);
                        let newCanister = await index.newCanister();
                        let s = {canister = newCanister; var innerKey: ?{key: InnerSubDBKey} = null};
                        inserting2.newInnerCanister := ?s;
                        (newCanister, s);
                    };
                };
                let newInnerSubDBKey = switch (newCanister.innerKey) {
                    case (?{key = newSubDBKey}) { newSubDBKey };
                    case (null) {
                        MyCycles.addPart(dbOptions.partitionCycles);
                        let {inner} = await canister.rawInsertSubDB(subDB.map, null, subDB.userData, dbOptions);
                        newCanister.innerKey := ?{key = inner};
                        inner;
                    }
                };

                // There was `isOverflowed`, change the outer.
                MyCycles.addPart(dbOptions.partitionCycles);
                await outerCanister.putLocation(outerKey, canister, newInnerSubDBKey);
                ignore BTree.delete(oldInnerSuperDB.subDBs, Nat.compare, oldInnerKey);

                (canister, newInnerSubDBKey);
            };
            case (null) {
                Debug.trap("no sub-DB");
            };
        };

        SparseQueue.delete(oldInnerSuperDB.inserting2, guid);
        result;
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
        MyCycles.addPart(options.dbOptions.partitionCycles);
        let pks = await options.index.getCanisters();
        let lastCanister = pks[pks.size()-1];
        MyCycles.addPart(options.dbOptions.partitionCycles);
        if (lastCanister == options.oldCanister and (await lastCanister.isOverflowed({dbOptions = options.dbOptions}))) {
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
        MyCycles.addPart(options.dbOptions.partitionCycles);
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

    // TODO: More fine-tuned lock: for individual sub-DB entries.
    func trapMoving({superDB: SuperDB; subDBKey: OuterSubDBKey; guid: SparseQueue.GUID}) {
        // If we call it repeatedly (with the same GUID), allow despite the lock.
        let v = BTree.get(superDB.busy, Nat.compare, subDBKey);
        if (v != null and v != ?guid) {
            Debug.trap("item busy");
        };
        // TODO: Can be optimized:
        ignore BTree.insert(superDB.busy, Nat.compare, subDBKey, guid);
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

    public type GetByOuterOptions = {dbOptions: DBOptions; outerSuperDB: SuperDB; outerKey: OuterSubDBKey; sk: SK};

    // Sometimes traps "missing sub-DB".
    public func getByOuter(options: GetByOuterOptions) : async* ?AttributeValue {
        let ?(part, inner) = getInner(options.outerSuperDB, options.outerKey) else {
            Debug.trap("no entry");
        };
        MyCycles.addPart(options.dbOptions.partitionCycles);
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
            return false;
        };
        return true;
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

    public type SubDBSizeByOuterOptions = {dbOptions: DBOptions; outerSuperDB: SuperDB; outerKey: OuterSubDBKey};

    public func subDBSizeByOuter(options: SubDBSizeByOuterOptions): async* ?Nat {
        let ?(part, innerKey) = getInner(options.outerSuperDB, options.outerKey) else {
            Debug.trap("no sub-DB");
        };
        MyCycles.addPart(options.dbOptions.partitionCycles);
        await part.subDBSizeByInner({subDBKey = innerKey});
    };

    /// To be called in a partition where `innerSuperDB` resides.
    public func startInsertingImpl(options: {
        guid: GUID;
        dbOptions: DBOptions;
        indexCanister: IndexCanister;
        outerCanister: PartitionCanister;
        outerKey: OuterSubDBKey;
        sk: SK;
        value: AttributeValue;
        innerSuperDB: SuperDB;
        innerKey: InnerSubDBKey;
    }) : async* () {
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
                    oldInnerSuperDB = options.innerSuperDB;
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
        : async* {inner: (PartitionCanister, InnerSubDBKey); outer: (PartitionCanister, OuterSubDBKey)} // TODO: need to return this value?
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

        trapMoving({superDB = options.outerSuperDB; subDBKey = options.outerKey; guid = options.guid});

        if (not inserting.insertingImplDone) {
            MyCycles.addPart(options.dbOptions.partitionCycles);
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
                // FIXME: I call `isOverflowed` second time, what is: 1. inefficient; 2. (?) inconsistent.
                MyCycles.addPart(options.dbOptions.partitionCycles);
                if (await oldInnerCanister.isOverflowed({dbOptions = options.dbOptions})) {
                    MyCycles.addPart(options.dbOptions.partitionCycles);
                    let (innerPartition, innerKey) = await oldInnerCanister.finishMovingSubDBImpl({
                        guid = options.guid; index = options.indexCanister; dbOptions = options.dbOptions;
                        oldInnerKey;
                        outerCanister = options.outerCanister;
                        outerKey = options.outerKey;
                    });
                    options.outerSuperDB.moving := null; // FIXME
                    (innerPartition, innerKey);
                } else {
                    (oldInnerCanister, oldInnerKey);
                }
            };
        };
        inserting.finishMovingSubDBDone := ?{ // TODO: seems unnecessary
            innerPartition = newInnerPartition;
            innerKey = newInnerKey;
        };

        SparseQueue.delete(options.outerSuperDB.inserting, options.guid);
        ignore BTree.delete(options.outerSuperDB.busy, Nat.compare, options.outerKey); // TODO: Don't repeat this line.

        {inner = (newInnerPartition, newInnerKey); outer = (options.outerCanister, options.outerKey)};
    };

    public func deleteInner({innerSuperDB: SuperDB; innerKey: InnerSubDBKey; sk: SK}): async* () {
        switch (BTree.get(innerSuperDB.subDBs, Nat.compare, innerKey)) {
            case (?subDB) {
                subDB.map := RBT.delete<Text, AttributeValue>(subDB.map, Text.compare, sk);
            };
            case (null) {
                Debug.trap("no sub-DB")
            }
        }
    };

    type DeleteOptions = {dbOptions: DBOptions; outerSuperDB: SuperDB; outerKey: OuterSubDBKey; sk: SK; guid: GUID};
    
    /// idempotent
    public func delete(options: DeleteOptions): async* () {
        // FIXME: Use `guid` to allow multiple calls.
        trapMoving({superDB = options.outerSuperDB; subDBKey = options.outerKey; guid = options.guid});
        switch(getInner(options.outerSuperDB, options.outerKey)) {
            case (?(innerCanister, innerKey)) {
                MyCycles.addPart(options.dbOptions.partitionCycles);
                await innerCanister.deleteInner(innerKey, options.sk);
            };
            case (null) {};
        };
        ignore BTree.delete(options.outerSuperDB.busy, Nat.compare, options.outerKey); // TODO: Don't repeat this line.
    };

    type DeleteDBOptions = {dbOptions: DBOptions; outerSuperDB: SuperDB; outerKey: OuterSubDBKey; guid: GUID};
    
    public func deleteSubDB(options: DeleteDBOptions): async* () {
        trapMoving({superDB = options.outerSuperDB; subDBKey = options.outerKey; guid = options.guid});

        switch(getInner(options.outerSuperDB, options.outerKey)) {
            case (?(innerCanister, innerKey)) {
                MyCycles.addPart(options.dbOptions.partitionCycles);
                await innerCanister.deleteSubDBInner(innerKey);
            };
            case (null) {};
        };
        ignore BTree.delete(options.outerSuperDB.locations, Nat.compare, options.outerKey);

        ignore BTree.delete(options.outerSuperDB.busy, Nat.compare, options.outerKey); // TODO: Don't repeat this line.
    };

    public func deleteSubDBInner(superDB: SuperDB, innerKey: InnerSubDBKey) : async* () {
        ignore BTree.delete(superDB.subDBs, Nat.compare, innerKey);
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
        let creating0: CreatingSubDB = {var canister = null; var loc = null; userData};
        let creating = SparseQueue.add(dbIndex.creatingSubDB, guid, creating0);
        let part3: PartitionCanister = switch (creating.canister) { // both inner and outer
            case (?part) { part };
            case (null) {
                let canisters = StableBuffer.toArray(dbIndex.canisters); // TODO: a special function for this
                let part = canisters[canisters.size() - 1];
                MyCycles.addPart(dbOptions.partitionCycles);
                let part2 = if (await part.isOverflowed({dbOptions})) { // TODO: Join .isOverflowed and .newCanister into one call?
                    let part2 = await* newCanister(dbOptions, dbIndex);
                    creating.canister := ?part;
                    part2;
                } else {
                    let {inner; outer} = switch (creating.loc) {
                        case (?loc) { loc };
                        case (null) {
                            MyCycles.addPart(dbOptions.partitionCycles);
                            let {inner; outer} = await part.rawInsertSubDBAndSetOuter(part, RBT.init(), null, creating.userData, dbOptions);
                            creating.loc := ?{inner = (part, inner); outer = (part, outer)};
                            {inner = (part, inner); outer = (part, outer)};
                        };
                    };
                    part;
                };
            };
        };
        let {inner; outer} = switch (creating.loc) {
            case (?loc) { loc };
            case (null) {
                MyCycles.addPart(dbOptions.partitionCycles);
                let {inner; outer} = await part3.rawInsertSubDBAndSetOuter(part3, RBT.init(), null, creating.userData, dbOptions);
                creating.loc := ?{inner = (part3, inner); outer = (part3, outer)};
                {inner = (part3, inner); outer = (part3, outer)};
            };
        };
        SparseQueue.delete(dbIndex.creatingSubDB, guid);
        {inner; outer}
    };

    /// In the current version two partition canister are always the same.
    ///
    /// `superDB` should reside in `part`.
    public func createOuter(outerSuperDB: SuperDB, part: PartitionCanister, outerKey: OuterSubDBKey, innerKey: InnerSubDBKey)
        : {inner: (PartitionCanister, InnerSubDBKey); outer: (PartitionCanister, OuterSubDBKey)}
    {
        ignore BTree.insert(outerSuperDB.locations, Nat.compare, outerKey, (part, innerKey));
        {inner = (part, innerKey); outer = (part, outerKey)};
    };

    // Scanning/enumerating //

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
                // Debug.print("isize: " # debug_show(RBT.size(subDB.map)));
                RBT.scanLimit(subDB.map, Text.compare, options.lowerBound, options.upperBound, options.dir, options.limit);
            };
            case (null) {
                Debug.trap("missing sub-DB");
            };
        };
    };

    type ScanLimitOuterOptions = {dbOptions: DBOptions; outerSuperDB: SuperDB; outerKey: OuterSubDBKey; lowerBound: Text; upperBound: Text; dir: RBT.Direction; limit: Nat};
    
    public func scanLimitOuter(options: ScanLimitOuterOptions): async* RBT.ScanLimitResult<Text, AttributeValue> {
        let ?(part, innerKey) = getInner(options.outerSuperDB, options.outerKey) else {
            Debug.trap("no sub-DB");
        };
        MyCycles.addPart(options.dbOptions.partitionCycles);
        await part.scanLimitInner({innerKey; lowerBound = options.lowerBound; upperBound = options.upperBound; dir = options.dir; limit = options.limit});
    };

    public func scanSubDBs({superDB: SuperDB}): [(OuterSubDBKey, (PartitionCanister, InnerSubDBKey))] {
        Iter.toArray(BTree.entries(superDB.locations));
    };

    /// Canisters

    public func getCanisters(dbIndex: DBIndex): [PartitionCanister] {
        StableBuffer.toArray(dbIndex.canisters);
    };

    public func newCanister(dbOptions: DBOptions, dbIndex: DBIndex): async* PartitionCanister {
        MyCycles.addPart(dbOptions.partitionCycles);
        let canister = await dbOptions.constructor(dbOptions);
        StableBuffer.add(dbIndex.canisters, canister); // TODO: too low level
        canister;
    };


};