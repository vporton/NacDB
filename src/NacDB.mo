/// This is a library for a multicanister DB consisting of sub-DBs, each fitting inside
/// a canister. The advantage of this library over other DBs libraries is:
///
/// * The summary size of sub-DBs can exceed the size of a canister.
/// * Each sub-DB can be seamlessly and efficently enumerated.
///
/// This library is indespensable for such use cases as:
///
/// * a social network with lists of posts
/// * a grant application with lists of grants
/// * NFTs site with lists of NFTs
///
/// For example of using this, see `index` and `partition` in the `src/` directory of this project.
/// (Don't forget to add authorization to these example actor, when you build on that examples.)
/// You actually use not this module, but examples using it.
///
/// Some functions in this module take GUID argument.
/// If such a function fails, you can call it with the same GUID again.
/// But better you can call `*Finish` method to finish its execution.
///
/// If you want also to reorder elements in lists, use `nacdb-reorder` (https://mops.one/nacdb-reorder).
/// `nacdb-reorder` uses two `nacdb` sub-DBs per each list, to track the order.

import Result "mo:base/Result";
import I "mo:base/Iter";
import Principal "mo:base/Principal";
import BTree "mo:stableheapbtreemap/BTree";
import RBT "mo:stable-rbtree/StableRBTree";
import StableBuffer "mo:stable-buffer/StableBuffer";
import Text "mo:base/Text";
import Nat "mo:base/Nat";
import Prim "mo:⛔";
import Debug "mo:base/Debug";
import OpsQueue "./OpsQueue";
import MyCycles "./Cycles";
import Blob "mo:base/Blob";

module {

    /// A globally unique identifier. This library uses 128-bit GUIDs.
    public type GUID = Blob;

    /// A key identifying an entry (either an "inner key" or "outer key", see below).
    public type SubDBKey = Nat;

    /// The "inner" key identifying a sub-DB stored in a canister.
    /// When a new sub-DB is inserted, this key may change.
    /// So, you are usually recommended to use "outer key" instead.
    public type InnerSubDBKey = SubDBKey;

    /// The key identifying a sub-DB stored in a canister.
    /// It's a constant (regarding moving a sub-DB to another canister) key mapped to `InnerSubDBKey`.
    public type OuterSubDBKey = SubDBKey;

    /// A key for accessing an entry in a sub-DB.
    public type SK = Text;

    /// A subkey type for accessing a part of an entry in a sub-DB.
    public type AttributeKey = Text;

    // TODO: I've commented out some types due to https://github.com/dfinity/motoko/issues/4213
    public type AttributeValuePrimitive = {#text : Text; #int : Int; #bool : Bool; #float : Float};
    // public type AttributeValueBlob = {#blob : Blob};
    public type AttributeValueTuple = {#tuple : [AttributeValuePrimitive]};
    public type AttributeValueArray = {#arrayText : [Text]; #arrayInt : [Int]; #arrayBool : [Bool]; #arrayFloat : [Float]};
    // public type AttributeValueRBTreeValue = AttributeValuePrimitive or /*AttributeValueBlob or*/ AttributeValueTuple or AttributeValueArray;
    // public type AttributeValueRBTree = {#tree : RBT.Tree<Text, AttributeValueRBTreeValue>};

    /// The value of an entry in a sub-DB.
    public type AttributeValue = AttributeValuePrimitive or /*AttributeValueBlob or*/ AttributeValueTuple or AttributeValueArray /*or AttributeValueRBTree*/;

    /// A sub-DB, as identified by inner key.
    ///
    /// Threat as an opaque type.
    public type SubDB = {
        var map: BTree.BTree<SK, AttributeValue>;
        var userData: Text; // useful to have a back reference to "locator" of our sub-DB in another database
        var hardCap: ?Nat;
    };

    /// Pair of a canister and a sub-DB key in it.
    public type Pair = {canister: PartitionCanister; key: SubDBKey};

    /// Pair of a canister and an outer sub-DB key in it.
    public type OuterPair = {canister: OuterCanister; key: OuterSubDBKey};

    /// Pair of a canister and an inner sub-DB key in it.
    public type InnerPair = {canister: InnerCanister; key: InnerSubDBKey};

    /// After using that much memory in a partition canister, create a new canister.
    public type MoveCap = { #usedMemory: Nat };

    /// Arguments for `createSubDB`.
    public type CreatingSubDBOptions = {index: IndexCanister; dbIndex: DBIndex; userData: Text; hardCap: ?Nat};

    /// Internal.
    public type CreatingSubDB = {
        options: CreatingSubDBOptions;
        var canister: ?PartitionCanister; // Immediately after creation of sub-DB, this is both inner and outer.
        var loc: ?{inner: InnerPair; outer: OuterPair};
    };

    /// Treat this as an opaque data structure, because this data is ignored if the sub-DB moves during insertion.
    public type InsertingItem = {
        options: InsertOptions;
        subDBKey: OuterSubDBKey;
        var needsMove: ?Bool;
        var insertingImplDone: Bool;
        var finishMovingSubDBDone: ?{
            newInnerPartition: InnerCanister;
            newInnerKey: OuterSubDBKey;
        };
        var newInnerCanister: ?{
            canister: InnerCanister;
            var innerKey: ?InnerSubDBKey;
        };
    };

    /// A super-DB is a structure inside a partition canister that keeps account of sub-DBs.
    ///
    /// Threat it as an opaque type.
    public type SuperDB = {
        dbOptions: DBOptions;
        var nextKey: Nat;
        subDBs: BTree.BTree<InnerSubDBKey, SubDB>;
        /// `inner` of this `RBT.Tree` is constant,
        /// even when the sub-DB to which it points moves to a different canister.
        var locations: BTree.BTree<OuterSubDBKey, {inner: InnerPair; /*var busy: ?OpsQueue.GUID*/}>;
    };

    /// This structure is intended to exist only one per the entire database.
    ///
    /// Threat it as an opaque type.
    public type DBIndex = {
        dbOptions: DBOptions;
        var canisters: StableBuffer.StableBuffer<PartitionCanister>;
        creatingSubDB: OpsQueue.OpsQueue<CreatingSubDB, CreateSubDBResult>;
        deletingSubDB: OpsQueue.OpsQueue<DeletingSubDB, ()>;
        inserting: OpsQueue.OpsQueue<InsertingItem, InsertResult>;  // outer
        deleting: OpsQueue.OpsQueue<DeletingItem, ()>;
        moving: BTree.BTree<OuterPair, ()>;
        blockDeleting: BTree.BTree<OuterPair, ()>; // used to prevent insertion after DB deletion
    };

    /// This canister is intended to exist only one per the entire database.
    public type IndexCanister = actor {
        createPartition: shared() -> async Principal;
        getCanisters: query () -> async [Principal];
        createSubDB: shared(guid: [Nat8], {userData: Text; hardCap: ?Nat})
            -> async {inner: {canister: Principal; key: InnerSubDBKey}; outer: {canister: Principal; key: OuterSubDBKey}};
        insert(guid: [Nat8], {
            outerCanister: Principal;
            outerKey: OuterSubDBKey;
            sk: SK;
            value: AttributeValue;
            hardCap: ?Nat;
        }) : async Result.Result<{inner: {canister: Principal; key: InnerSubDBKey}; outer: {canister: Principal; key: OuterSubDBKey}}, Text>;
        delete(guid: [Nat8], {outerCanister: Principal; outerKey: OuterSubDBKey; sk: SK}): async ();
        deleteSubDB(guid: [Nat8], {outerCanister: Principal; outerKey: OuterSubDBKey}) : async ();
    };

    /// This canister with sub-DBs (identified by an inner and an outer key).
    public type PartitionCanister = actor {
        // Mandatory //
        rawGetSubDB: query ({innerKey: InnerSubDBKey}) -> async ?{map: [(SK, AttributeValue)]; userData: Text};
        rawDeleteSubDB: ({innerKey: InnerSubDBKey}) -> async ();
        rawInsertSubDB({map: [(SK, AttributeValue)]; innerKey: ?InnerSubDBKey; userData: Text; hardCap: ?Nat})
            : async {innerKey: InnerSubDBKey};
        rawInsertSubDBAndSetOuter({
            map: [(SK, AttributeValue)];
            keys: ?{
                innerKey: InnerSubDBKey;
                outerKey: OuterSubDBKey;
            };
            userData: Text;
            hardCap: ?Nat;
        })
            : async {innerKey: InnerSubDBKey; outerKey: OuterSubDBKey};
        getInner: query ({outerKey: OuterSubDBKey}) -> async ?{canister: Principal; key: InnerSubDBKey};
        isOverflowed: query () -> async Bool;
        putLocation({outerKey: OuterSubDBKey; innerCanister: Principal; newInnerSubDBKey: InnerSubDBKey}) : async ();
        // In the current version two partition canister are always the same.
        createOuter({part: Principal; outerKey: OuterSubDBKey; innerKey: InnerSubDBKey})
            : async {inner: {canister: Principal; key: InnerSubDBKey}; outer: {canister: Principal; key: OuterSubDBKey}};
        startInsertingImpl(options: {
            innerKey: InnerSubDBKey;
            sk: SK;
            value: AttributeValue;
        }): async ();
        deleteSubDBOuter({outerKey: OuterSubDBKey}): async ();

        // Optional //

        superDBSize: query () -> async Nat;
        deleteSubDBInner({innerKey: InnerSubDBKey}) : async ();
        deleteInner({innerKey: InnerSubDBKey; sk: SK}): async ();
        scanLimitInner: query({innerKey: InnerSubDBKey; lowerBound: SK; upperBound: SK; dir: RBT.Direction; limit: Nat})
            -> async RBT.ScanLimitResult<Text, AttributeValue>;
        scanLimitOuter: shared({outerKey: OuterSubDBKey; lowerBound: SK; upperBound: SK; dir: RBT.Direction; limit: Nat})
            -> async RBT.ScanLimitResult<Text, AttributeValue>;
        getByInner: query (options: {innerKey: InnerSubDBKey; sk: SK}) -> async ?AttributeValue;
        hasByInner: query (options: {innerKey: InnerSubDBKey; sk: SK}) -> async Bool;
        getByOuter: shared (options: {outerKey: OuterSubDBKey; sk: SK}) -> async ?AttributeValue;
        hasByOuter: shared (options: {outerKey: OuterSubDBKey; sk: SK}) -> async Bool;
        hasSubDBByInner: query (options: {innerKey: InnerSubDBKey}) -> async Bool;
        hasSubDBByOuter: shared (options: {outerKey: OuterSubDBKey}) -> async Bool;
        subDBSizeByInner: query (options: {innerKey: InnerSubDBKey}) -> async ?Nat;
        subDBSizeByOuter: shared (options: {outerKey: OuterSubDBKey}) -> async ?Nat;
        scanSubDBs: query() -> async [(OuterSubDBKey, {canister: Principal; key: InnerSubDBKey})];
        getSubDBUserDataInner: shared (options: {innerKey: InnerSubDBKey}) -> async ?Text;
        getOuter: shared GetByOuterPartitionKeyOptions -> async ?AttributeValue;
        getSubDBUserDataOuter: shared GetUserDataOuterOptions -> async ?Text;
        // hasByOuterPartitionKey: shared HasByOuterPartitionKeyOptions -> async Bool;
        subDBSizeOuterImpl : shared SubDBSizeOuterOptions -> async ?Nat;
    };

    /// A canister as identified by an inner key.
    public type InnerCanister = PartitionCanister;

    /// A canister as identified by an outer key.
    public type OuterCanister = PartitionCanister;

    /// Initialize `DBIndex` structure (done once per creation of the entire database).
    public func createDBIndex(dbOptions: DBOptions) : DBIndex {
        {
            var canisters = StableBuffer.init<PartitionCanister>();
            creatingSubDB = OpsQueue.init(dbOptions.createDBQueueLength);
            dbOptions;
            inserting = OpsQueue.init(dbOptions.insertQueueLength);
            deleting = OpsQueue.init(dbOptions.insertQueueLength);
            deletingSubDB = OpsQueue.init(dbOptions.insertQueueLength);
            moving = BTree.init(null);
            blockDeleting = BTree.init(null);
        };
    };

    /// Create a `SuperDB`.
    ///
    /// An internal function.
    public func createSuperDB(dbOptions: DBOptions) : SuperDB {
        {
            dbOptions;
            var nextKey = 0;
            subDBs = BTree.init<InnerSubDBKey, SubDB>(null);
            var locations = BTree.init(null);
        };
    };

    /// Options for the DB.
    public type DBOptions = {
        moveCap: MoveCap;
        partitionCycles: Nat;
        createDBQueueLength: Nat;
        insertQueueLength: Nat;
    };

    /// Internal.
    public func rawGetSubDB(
        superDB: SuperDB,
        innerKey: InnerSubDBKey,
    ) : ?{map: [(SK, AttributeValue)]; userData: Text}
    {
        do ? {
            let e = BTree.get(superDB.subDBs, Nat.compare, innerKey)!;
            {map = BTree.toArray(e.map); userData = e.userData}
        };
    };

    /// The "real" returned value is `outer`, but `inner` can be used for caching
    /// (on cache failure retrieve new `inner` using `outer`).
    ///
    /// Internal.
    public func rawInsertSubDB({
        superDB: SuperDB;
        map: [(SK, AttributeValue)];
        innerKey: ?InnerSubDBKey;
        userData: Text;
        hardCap: ?Nat;
    }) : {innerKey: InnerSubDBKey}
    {
        let innerKey2 = switch (innerKey) {
            case (?innerKey) { innerKey };
            case (null) {
                let key = superDB.nextKey;
                superDB.nextKey += 1;
                key;
            };
        };                    
        let subDB : SubDB = {
            var map = BTree.fromArray(8, Text.compare, map);
            var userData = userData;
            var hardCap = hardCap;
        };
        ignore BTree.insert(superDB.subDBs, Nat.compare, innerKey2, subDB);
        {innerKey = innerKey2};
    };

    /// Internal.
    public func rawDeleteSubDB(superDB: SuperDB, innerKey: InnerSubDBKey) : () {
        ignore BTree.delete(superDB.subDBs, Nat.compare, innerKey);
    };

    /// Use only if sure that outer and inner canisters coincide.
    ///
    /// Internal.
    public func rawInsertSubDBAndSetOuter({
        superDB: SuperDB;
        canister: InnerCanister;
        map: [(SK, AttributeValue)];
        keys: ?{
            innerKey: InnerSubDBKey;
            outerKey: OuterSubDBKey;
        };
        userData: Text;
        hardCap: ?Nat;
    }) : {outerKey: OuterSubDBKey; innerKey: InnerSubDBKey}
    {
        let {innerKey = innerKey2} = rawInsertSubDB({superDB; map; innerKey = do ? {keys!.innerKey}; userData; hardCap});
        if (keys == null) {
            ignore BTree.insert(superDB.locations, Nat.compare, superDB.nextKey,
                {inner = {canister; key = innerKey2}; /*var busy: ?OpsQueue.GUID = null*/});
        };
        switch (keys) {
            case (?{innerKey; outerKey}) {
                {outerKey; innerKey};
            };
            case (null) {
                let result = {outerKey = superDB.nextKey; innerKey = innerKey2; };
                superDB.nextKey += 1;
                result;
            };
        };
    };

    /// Transform the outer key into an inner key.
    ///
    /// Note that the inner key may change after an insert operation.
    public func getInner({superDB: SuperDB; outerKey: OuterSubDBKey}) : ?InnerPair {
        do ? {
            BTree.get(superDB.locations, Nat.compare, outerKey)!.inner;
        }
    };

    /// Internal.
    public func getSubDBByInner(superDB: SuperDB, innerKey: InnerSubDBKey) : ?SubDB {
        BTree.get(superDB.subDBs, Nat.compare, innerKey);
    };

    /// Internal.
    public func putLocation({outerSuperDB: SuperDB; outerKey: OuterSubDBKey; innerCanister: InnerCanister; innerKey: InnerSubDBKey}) {
        ignore BTree.insert(outerSuperDB.locations, Nat.compare, outerKey,
            {inner = {canister = innerCanister; key = innerKey}; /*var busy: ?OpsQueue.GUID = null*/});
    };

    /// This function makes no sense, because it would return the entire sub-DB from another canister.
    // public func getSubDBByOuter(superDB: SuperDB, outerKey: OuterSubDBKey) : ?SubDB {
    // };

    /// Called only if `isOverflowed`.
    ///
    /// Internal.
    public func finishMovingSubDBImpl({
        inserting: InsertingItem;
        dbIndex: DBIndex;
        index: IndexCanister;
        outerCanister: OuterCanister;
        outerKey: OuterSubDBKey;
        oldInnerCanister: InnerCanister;
        oldInnerKey: InnerSubDBKey;
    }) : async* InnerPair
    {
        MyCycles.addPart<system>(dbIndex.dbOptions.partitionCycles);
        let result = switch (await oldInnerCanister.rawGetSubDB({innerKey = oldInnerKey})) {
            case (?subDB) {
                let (canister, newCanister) = switch (inserting.newInnerCanister) {
                    case (?newCanister) { (newCanister.canister, newCanister) };
                    case (null) {
                        MyCycles.addPart<system>(dbIndex.dbOptions.partitionCycles);
                        let newCanister0 = await* createPartitionImpl(index, dbIndex);
                        let newCanister: PartitionCanister = actor(Principal.toText(newCanister0));
                        let s = {canister = newCanister; var innerKey: ?InnerSubDBKey = null};
                        inserting.newInnerCanister := ?s;
                        (newCanister, s);
                    };
                };
                let newInnerSubDBKey = switch (newCanister.innerKey) {
                    case (?newSubDBKey) { newSubDBKey };
                    case (null) {
                        if (BTree.has(dbIndex.moving, compareLocs, {canister = outerCanister; key = outerKey})) {
                            Debug.trap("DB is scaling");
                        };
                        MyCycles.addPart<system>(dbIndex.dbOptions.partitionCycles);
                        let {innerKey} = await canister.rawInsertSubDB({
                            map = subDB.map;
                            innerKey = null;
                            userData = subDB.userData;
                            hardCap = inserting.options.hardCap;
                        });
                        newCanister.innerKey := ?innerKey;
                        innerKey;
                    }
                };

                // There was `isOverflowed`, change the outer.
                MyCycles.addPart<system>(dbIndex.dbOptions.partitionCycles);
                await outerCanister.putLocation({outerKey; innerCanister = Principal.fromActor(canister); newInnerSubDBKey});
                MyCycles.addPart<system>(dbIndex.dbOptions.partitionCycles);
                await oldInnerCanister.rawDeleteSubDB({innerKey = oldInnerKey});

                {canister; key = newInnerSubDBKey};
            };
            case (null) {
                Debug.trap("no sub-DB");
            };
        };

        result;
    };

    /// Check whether a canister with a given sub-DB takes too much memory
    /// (and there is the need to create a new canister to move this sub-DB to).
    ///
    /// Internal.
    public func isOverflowed({superDB: SuperDB}) : Bool {
        switch (superDB.dbOptions.moveCap) {
            case (#usedMemory mem) {
                Prim.rts_heap_size() > mem; // current canister
            };
        };
    };

    // func releaseOuterKey(outerSuperDB: SuperDB, outerKey: OuterSubDBKey) {
    //     switch (BTree.get(outerSuperDB.locations, Nat.compare, outerKey)) {
    //         case (?item) {
    //             item.busy := null;
    //         };
    //         case (null) {};
    //     };
    // };

    func removeLoosers({subDB: SubDB}) {
        switch (subDB.hardCap) {
            case (?hardCap) {
                while (BTree.size(subDB.map) > hardCap) {
                    let iter = BTree.entries(subDB.map);
                    switch (iter.next()) {
                        case (?(k, v)) {
                            ignore BTree.delete(subDB.map, Text.compare, k);
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

    public type GetByInnerOptions = {superDB: SuperDB; innerKey: InnerSubDBKey; sk: SK};

    /// Get a sub-DB entry by its keys.
    public func getByInner(options: GetByInnerOptions) : ?AttributeValue {
        switch (getSubDBByInner(options.superDB, options.innerKey)) {
            case (?subDB) {
                BTree.get(subDB.map, Text.compare, options.sk);
            };
            case (null) {
                Debug.trap("missing sub-DB")
            }
        }
    };

    public type GetByOuterOptions = {outerSuperDB: SuperDB; outerKey: OuterSubDBKey; sk: SK};

    /// Get a sub-DB entry by its keys.
    ///
    /// Sometimes traps "missing sub-DB". Needs to be repeated in this case.
    public func getByOuter(options: GetByOuterOptions) : async* ?AttributeValue {
        let ?{canister = part; key} = getInner({outerKey = options.outerKey; superDB = options.outerSuperDB}) else {
            Debug.trap("no entry");
        };
        MyCycles.addPart<system>(options.outerSuperDB.dbOptions.partitionCycles);
        await part.getByInner({innerKey = key; sk = options.sk});
    };

    public type GetByOuterPartitionKeyOptions = {outer: OuterPair; sk: SK};

    /// Get a sub-DB entry by its keys.
    public func getOuter(options: GetByOuterPartitionKeyOptions, dbOptions: DBOptions) : async* ?AttributeValue {
        MyCycles.addPart<system>(dbOptions.partitionCycles);
        await options.outer.canister.getByOuter({outerKey = options.outer.key; sk = options.sk});
    };

    public type ExistsByInnerOptions = GetByInnerOptions;

    /// Check whether a sub-DB entry with this key exists.
    public func hasByInner(options: ExistsByInnerOptions) : Bool {
        getByInner(options) != null;
    };

    public type ExistsByOuterOptions = GetByOuterOptions;

    /// Check whether a sub-DB entry with this key exists.
    ///
    /// Sometimes traps "missing sub-DB". Needs to be repeated in this case.
    public func hasByOuter(options: ExistsByOuterOptions) : async* Bool {
        (await* getByOuter(options)) != null;
    };

    // public type HasByOuterPartitionKeyOptions = GetByOuterPartitionKeyOptions;

    // public func hasByOuterPartitionKey(options: HasByOuterPartitionKeyOptions) : async* Bool {
    //     await options.outer.canister.hasByOuter({outerKey = options.outer.key; sk = options.sk});
    // };

    public type HasDBByInnerOptions = {innerSuperDB: SuperDB; innerKey: InnerSubDBKey};

    /// Check whether a sub-DB exits by its inner key.
    public func hasSubDBByInner(options: HasDBByInnerOptions) : Bool {
        BTree.has(options.innerSuperDB.subDBs, Nat.compare, options.innerKey);
    };

    public type HasDBByOuterOptions = {outerSuperDB: SuperDB; outerKey: OuterSubDBKey};

    /// Check whether a sub-DB exits by its outer key.
    public func hasSubDBByOuter(options: HasDBByOuterOptions) : async* Bool {
        let ?{canister = _part; key = _inner} = getInner({outerKey = options.outerKey; superDB = options.outerSuperDB}) else {
            return false;
        };
        return true;
    };

    public type GetUserDataOuterOptions = {outer: OuterPair};

    /// Get a sub-DB "user-data" by its outer key.
    public func getSubDBUserDataOuter(options: GetUserDataOuterOptions, dbOptions: DBOptions) : async* ?Text {
        MyCycles.addPart<system>(dbOptions.partitionCycles);
        await options.outer.canister.getSubDBUserDataOuter(options);
    };

    public type GetUserDataInnerOptions = {superDB: SuperDB; subDBKey: InnerSubDBKey};

    /// Get a sub-DB "user-data" by its inner key.
    public func getSubDBUserDataInner(options: GetUserDataInnerOptions) : ?Text {
        do ? { BTree.get(options.superDB.subDBs, Nat.compare, options.subDBKey)!.userData };
    };

    /// Determine the size of a super-DB.
    public func superDBSize(superDB: SuperDB): Nat = BTree.size(superDB.subDBs);

    public type SubDBSizeByInnerOptions = {superDB: SuperDB; innerKey: InnerSubDBKey};

    /// Determine the size of a sub-DB by its inner key.
    public func subDBSizeByInner(options: SubDBSizeByInnerOptions): ?Nat {
        do ? {
            BTree.size(getSubDBByInner(options.superDB, options.innerKey)!.map);
        }
    };

    public type SubDBSizeByOuterOptions = {outerSuperDB: SuperDB; outerKey: OuterSubDBKey};

    /// Determine the size of a sub-DB by its outer key.
    public func subDBSizeByOuter(options: SubDBSizeByOuterOptions): async* ?Nat {
        let ?{canister = part; key = innerKey} = getInner({outerKey = options.outerKey; superDB = options.outerSuperDB}) else {
            Debug.trap("no sub-DB");
        };
        MyCycles.addPart<system>(options.outerSuperDB.dbOptions.partitionCycles);
        await part.subDBSizeByInner({innerKey});
    };

    public type SubDBSizeOuterOptions = {outer: OuterPair};

    /// Internal.
    public func subDBSizeOuterImpl(options: SubDBSizeOuterOptions, dbOptions: DBOptions): async* ?Nat {
        MyCycles.addPart<system>(dbOptions.partitionCycles);
        await options.outer.canister.subDBSizeByOuter({outerKey = options.outer.key});
    };

    /// To be called in a partition where `innerSuperDB` resides.
    ///
    /// Internal.
    public func startInsertingImpl(options: {
        innerKey: InnerSubDBKey;
        sk: SK;
        value: AttributeValue;
        innerSuperDB: SuperDB;
    }) : async* () {
        switch (getSubDBByInner(options.innerSuperDB, options.innerKey)) {
            case (?subDB) {
                ignore BTree.insert(subDB.map, Text.compare, options.sk, options.value);
                removeLoosers({subDB});
            };
            case (null) {
                Debug.trap("missing sub-DB");
            };
        };
    };

    public type InsertOptions = {
        indexCanister: Principal;
        dbIndex: DBIndex;
        outerCanister: Principal;
        outerKey: OuterSubDBKey;
        sk: SK;
        value: AttributeValue;
        hardCap: ?Nat;
    };

    public type InsertResult = Result.Result<{inner: InnerPair; outer: OuterPair}, Text>; // TODO: need to return this value?

    /// Insert an entry to a sub-DB.
    ///
    /// There is no `insertByInner`, because inserting may need to move the sub-DB.
    /// TODO: Modify TypeScript code accordingly.
    public func insert(guid: GUID, options: InsertOptions) : async* InsertResult {
        ignore OpsQueue.whilePending(options.dbIndex.inserting, func(guid: GUID, elt: InsertingItem): async* () {
            OpsQueue.answer(
                options.dbIndex.inserting,
                guid,
                await* insertFinishByQueue(guid, elt));
        });

        let outer: OuterCanister = actor(Principal.toText(options.outerCanister));
        let inserting = switch (OpsQueue.get(options.dbIndex.inserting, guid)) {
            case (?inserting) { inserting };
            case (null) {
                let inserting: InsertingItem = {
                    options;
                    part = options.outerCanister;
                    subDBKey = options.outerKey;
                    var needsMove = null;
                    var insertingImplDone = false;
                    var finishMovingSubDBDone = null;
                    var newInnerCanister = null;
                };

                if (BTree.has(options.dbIndex.blockDeleting, compareLocs, {canister = outer; key = options.outerKey})) {
                    Debug.trap("blocking deleting");
                };
                ignore BTree.insert(options.dbIndex.blockDeleting, compareLocs, {canister = outer; key = options.outerKey}, ());
                inserting;
            };
        };
        try {
            await* insertFinishByQueue(guid, inserting);
        }
        catch(e) {
            OpsQueue.add(options.dbIndex.inserting, guid, inserting);
            throw e;
        };
    };

    public func insertFinish(guid: GUID, dbIndex: DBIndex) : async* ?InsertResult {
        let result = OpsQueue.result(dbIndex.inserting, guid);
        result;
    };

    func insertFinishByQueue(guid: GUID, inserting: InsertingItem) : async* InsertResult {
        let outer: OuterCanister = actor(Principal.toText(inserting.options.outerCanister)); // TODO: duplicate operation

        MyCycles.addPart<system>(inserting.options.dbIndex.dbOptions.partitionCycles);
        let ?{canister = oldInnerPrincipal; key = oldInnerKey} = await outer.getInner({outerKey = inserting.options.outerKey}) else {
            ignore BTree.delete(inserting.options.dbIndex.blockDeleting, compareLocs, {
                canister = outer;
                key = inserting.options.outerKey;
            });
            let result = #err "missing sub-DB";
            OpsQueue.answer(inserting.options.dbIndex.inserting, guid, result);
            return result;
        };
        let oldInnerCanister: InnerCanister = actor (Principal.toText(oldInnerPrincipal));

        if (not inserting.insertingImplDone) {
            let needsMove = switch(inserting.needsMove) {
                case(?needsMove) { needsMove };
                case(null) {
                    MyCycles.addPart<system>(inserting.options.dbIndex.dbOptions.partitionCycles);
                    let needsMove = await oldInnerCanister.isOverflowed();
                    inserting.needsMove := ?needsMove;
                    needsMove;
                };
            };
            MyCycles.addPart<system>(inserting.options.dbIndex.dbOptions.partitionCycles);
            await oldInnerCanister.startInsertingImpl({
                sk = inserting.options.sk;
                value = inserting.options.value;
                innerKey = oldInnerKey;
                needsMove;
            });
            if (needsMove) {
                if (BTree.has(inserting.options.dbIndex.moving, compareLocs, {
                    canister = actor(Principal.toText(inserting.options.outerCanister)): OuterCanister;
                    key = inserting.options.outerKey;
                })) {
                    // ignore BTree.delete(inserting.options.dbIndex.blockDeleting, compareLocs, (outer, inserting.options.outerKey));
                    Debug.trap("already moving");
                };
            };
            inserting.insertingImplDone := true;
        };

        // TODO: check `.moving`
        let (newInnerPartition, newInnerKey) = switch (inserting.finishMovingSubDBDone) {
            case (?{newInnerPartition; newInnerKey}) { (newInnerPartition, newInnerKey) };
            case (null) {
                let needsMove = switch(inserting.needsMove) {
                    case(?needsMove) { needsMove };
                    case(null) {
                        MyCycles.addPart<system>(inserting.options.dbIndex.dbOptions.partitionCycles);
                        let needsMove = await oldInnerCanister.isOverflowed();
                        inserting.needsMove := ?needsMove;
                        needsMove;
                    };
                };
                if (needsMove) {
                    MyCycles.addPart<system>(inserting.options.dbIndex.dbOptions.partitionCycles);
                    let index: IndexCanister = actor(Principal.toText(inserting.options.indexCanister));
                    let {canister = innerPartition; key = innerKey} = await* finishMovingSubDBImpl({
                        inserting;
                        dbIndex = inserting.options.dbIndex;
                        index = actor(Principal.toText(inserting.options.indexCanister));
                        oldInnerKey;
                        outerCanister = actor(Principal.toText(inserting.options.outerCanister));
                        outerKey = inserting.options.outerKey;
                        oldInnerCanister;
                    });
                    ignore BTree.delete(inserting.options.dbIndex.moving, compareLocs, {
                        canister = actor(Principal.toText(inserting.options.outerCanister)): OuterCanister;
                        key = inserting.options.outerKey;
                    });
                    (innerPartition, innerKey);
                } else {
                    (oldInnerCanister, oldInnerKey);
                }
            };
        };
        inserting.finishMovingSubDBDone := ?{
            newInnerPartition;
            newInnerKey;
        };

        ignore BTree.delete(inserting.options.dbIndex.blockDeleting, compareLocs, {
            canister = outer;
            key = inserting.options.outerKey;
        });

        #ok {inner = {canister = newInnerPartition; key = newInnerKey}; outer = {canister = outer; key = inserting.options.outerKey}};
    };

    /// Delete an entry from a sub-DB.
    public func deleteInner({innerSuperDB: SuperDB; innerKey: InnerSubDBKey; sk: SK}): async* () {
        switch (BTree.get(innerSuperDB.subDBs, Nat.compare, innerKey)) {
            case (?subDB) {
                ignore BTree.delete<Text, AttributeValue>(subDB.map, Text.compare, sk);
            };
            case (null) {
                Debug.trap("no sub-DB")
            }
        }
    };

    type DeleteOptions = {dbIndex: DBIndex; outerCanister: OuterCanister; outerKey: OuterSubDBKey; sk: SK};
    
    /// Delete an entry from a sub-DB.
    ///
    /// idempotent
    public func delete(guid: GUID, options: DeleteOptions): async* () {
        ignore OpsQueue.whilePending<DeletingItem, ()>(options.dbIndex.deleting, func(guid: GUID, elt: DeletingItem): async* () {
            OpsQueue.answer(
                options.dbIndex.deleting,
                guid,
                await* deleteFinishByQueue(elt));
        });

        let deleting = switch (OpsQueue.get(options.dbIndex.deleting, guid)) {
            case (?deleting) { deleting };
            case null {
                let result = { options };
                if (BTree.has(options.dbIndex.blockDeleting, compareLocs, {
                    canister = options.outerCanister;
                    key = options.outerKey;
                })) {
                    Debug.trap("deleting is blocked");
                };
                ignore BTree.insert(options.dbIndex.blockDeleting, compareLocs, {
                    canister = options.outerCanister;
                    key = options.outerKey;
                }, ());
                result;
            };
        };

        try {
            await* deleteFinishByQueue(deleting);
        }
        catch(e) {
            OpsQueue.add(options.dbIndex.deleting, guid, deleting);
            throw e;
        };
    };

    type DeletingItem = {
        options: DeleteOptions;
    };

    public func deleteFinish(guid: GUID, dbIndex: DBIndex) : async* ?() {
        OpsQueue.result(dbIndex.deleting, guid);
    };

    func deleteFinishByQueue(deleting: DeletingItem) : async* () {
        switch(await deleting.options.outerCanister.getInner({outerKey = deleting.options.outerKey})) {
            case (?{canister = innerCanister; key = innerKey}) {
                let inner: InnerCanister = actor(Principal.toText(innerCanister));
                // Can we block here on inner key instead of outer one?
                MyCycles.addPart<system>(deleting.options.dbIndex.dbOptions.partitionCycles);
                await inner.deleteInner({innerKey; sk = deleting.options.sk});
            };
            case (null) {};
        };

        ignore BTree.delete(deleting.options.dbIndex.blockDeleting, compareLocs, {
            canister = deleting.options.outerCanister;
            key = deleting.options.outerKey;
        });
    };

    type DeleteDBOptions = {dbIndex: DBIndex; outerCanister: OuterCanister; outerKey: OuterSubDBKey};
    
    type DeletingSubDB = {options: DeleteDBOptions};

    /// Delete a sub-DB.
    public func deleteSubDB(guid: GUID, options: DeleteDBOptions): async* () {
        ignore OpsQueue.whilePending<DeletingSubDB, ()>(options.dbIndex.deletingSubDB, func(guid: GUID, elt: DeletingSubDB): async* () {
            OpsQueue.answer(
                options.dbIndex.deletingSubDB,
                guid,
                await* deleteSubDBFinishByQueue(elt));
        });
        let deleting = switch (OpsQueue.get(options.dbIndex.deletingSubDB, guid)) {
            case (?deleting) { deleting };
            case null {
                { options };
            };
        };

        try {
            await* deleteSubDBFinishByQueue(deleting);
        }
        catch(e) {
            OpsQueue.add(options.dbIndex.deletingSubDB, guid, deleting);
            throw e;
        };
    };

    // idempotent
    func deleteSubDBFinishByQueue(deleting: DeletingSubDB) : async* () {
        switch(await deleting.options.outerCanister.getInner({outerKey = deleting.options.outerKey})) {
            case (?{canister = innerCanister; key = innerKey}) {
                let inner: InnerCanister = actor(Principal.toText(innerCanister));
                MyCycles.addPart<system>(deleting.options.dbIndex.dbOptions.partitionCycles);
                await inner.deleteSubDBInner({innerKey});
            };
            case (null) {};
        };
        await deleting.options.outerCanister.deleteSubDBOuter({outerKey = deleting.options.outerKey});
    };

    public func deleteSubDBFinish(guid: GUID, dbIndex: DBIndex) : async* ?() {
        OpsQueue.result(dbIndex.deletingSubDB, guid);
    };

    /// Delete a sub-DB.
    public func deleteSubDBInner({superDB: SuperDB; innerKey: InnerSubDBKey}) : async* () {
        ignore BTree.delete(superDB.subDBs, Nat.compare, innerKey);
    };

    /// Delete a sub-DB.
    public func deleteSubDBOuter({superDB: SuperDB; outerKey: OuterSubDBKey}) : async* () {
        ignore BTree.delete(superDB.locations, Nat.compare, outerKey);
    };

    type DeleteDBPartitionKeyOptions = {outer: OuterPair; guid: GUID};

    // Creating sub-DB //

    type CreateSubDBOptions = {index: IndexCanister; dbIndex: DBIndex; userData: Text; hardCap: ?Nat};

    type CreateSubDBResult = {inner: InnerPair; outer: OuterPair};

    /// Create a sub-DB.
    ///
    /// It does not touch old items, so no locking.
    ///
    /// Pass a random GUID. Repeat the call with the same GUID, if the previous call failed.
    ///
    /// The "real" returned value is `outer`, but `inner` can be used for caching
    /// (on cache failure retrieve new `inner` using `outer`).
    ///
    /// In this version returned `PartitionCanister` for inner and outer always the same.
    public func createSubDB(guid: GUID, options: CreateSubDBOptions) : async* CreateSubDBResult {
        let creating: CreatingSubDB = switch (OpsQueue.get(options.dbIndex.creatingSubDB, guid)) {
            case (?creating) { creating };
            case (null) {
                {
                    options;
                    var canister = null;
                    var loc = null;
                };
            };
        };

        try {
            await* createSubDBFinishByQueue(creating);
        }
        catch(e) {
            OpsQueue.add<CreatingSubDB, CreateSubDBResult>(options.dbIndex.creatingSubDB, guid, creating);
            throw e;
        }
    };

    func createSubDBFinishByQueue(creating: CreatingSubDB) : async* CreateSubDBResult {
        let part3: PartitionCanister = switch (creating.canister) { // both inner and outer
            case (?part) { part };
            case (null) {
                let canisters = StableBuffer.toArray(creating.options.dbIndex.canisters);
                let part = canisters[canisters.size() - 1];
                MyCycles.addPart<system>(creating.options.dbIndex.dbOptions.partitionCycles);
                let part2 = if (await part.isOverflowed()) {
                    let part20 = await* createPartitionImpl(creating.options.index, creating.options.dbIndex);
                    let part2: PartitionCanister = actor(Principal.toText(part20));
                    creating.canister := ?part;
                    part2;
                } else {
                    let {inner; outer} = switch (creating.loc) {
                        case (?loc) { loc };
                        case (null) {
                            MyCycles.addPart<system>(creating.options.dbIndex.dbOptions.partitionCycles);
                            let {innerKey; outerKey} = await part.rawInsertSubDBAndSetOuter({
                                map = [];
                                keys = null;
                                userData = creating.options.userData;
                                hardCap = creating.options.hardCap;
                            });
                            creating.loc := ?{inner = {canister = part; key = innerKey}; outer = {canister = part; key = outerKey}};
                            {inner = (part, innerKey); outer = (part, outerKey)};
                        };
                    };
                    part;
                };
            };
        };
        switch (creating.loc) {
            case (?loc) { loc };
            case (null) {
                MyCycles.addPart<system>(creating.options.dbIndex.dbOptions.partitionCycles);
                let {innerKey; outerKey} = await part3.rawInsertSubDBAndSetOuter({
                    map = [];
                    keys = null;
                    userData = creating.options.userData;
                    hardCap = creating.options.hardCap;
                });
                creating.loc := ?{inner = {canister = part3; key = innerKey}; outer = {canister = part3; key = outerKey}};
                {inner = {canister = part3; key = innerKey}; outer = {canister = part3; key = outerKey}};
            };
        };
    };

    /// In the current version two partition canister are always the same.
    ///
    /// `superDB` should reside in `part`.
    ///
    /// Internal.
    public func createOuter({outerSuperDB: SuperDB; part: PartitionCanister; outerKey: OuterSubDBKey; innerKey: InnerSubDBKey})
        : {inner: InnerPair; outer: OuterPair}
    {
        ignore BTree.insert(outerSuperDB.locations, Nat.compare, outerKey,
            {inner = {canister = part; key = innerKey}; /*var busy: ?OpsQueue.GUID = null*/});
        {inner = {canister = part; key = innerKey}; outer = {canister = part; key = outerKey}};
    };

    // Scanning/enumerating //

    type IterInnerOptions = {innerSuperDB: SuperDB; innerKey: InnerSubDBKey};

    /// Get an iterator of sub-DB entries by its inner key.
    public func iterByInner(options: IterInnerOptions) : I.Iter<(Text, AttributeValue)> {
        switch (getSubDBByInner(options.innerSuperDB, options.innerKey)) {
            case (?subDB) {
                BTree.entries(subDB.map);
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
    
    /// Retrieve sub-DB entries by its inner key.
    public func entriesInner(options: EntriesInnerOptions) : I.Iter<(Text, AttributeValue)> {
        iterByInner({innerSuperDB = options.innerSuperDB; innerKey = options.innerKey; dir = #fwd});
    };

    // Impossible to implement.
    // type EntriesOuterOptions = {outerSuperDB: SuperDB; outerKey: OuterSubDBKey};
    
    // public func entriesOuter(options: EntriesOptions) : I.Iter<(Text, AttributeValue)> {
    // };

    type EntriesRevInnerOptions = {innerSuperDB: SuperDB; innerKey: InnerSubDBKey};
    
    /// Retrieve sub-DB entries in backward order by its inner key.
    public func entriesInnerRev(options: EntriesRevInnerOptions) : I.Iter<(Text, AttributeValue)> {
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
    
    /// Retrieve sub-DB entries by its inner key.
    public func scanLimitInner(options: ScanLimitInnerOptions): RBT.ScanLimitResult<Text, AttributeValue> {
        switch (getSubDBByInner(options.innerSuperDB, options.innerKey)) {
            case (?subDB) {
                // Debug.print("isize: " # debug_show(RBT.size(subDB.map)));
                BTree.scanLimit(subDB.map, Text.compare, options.lowerBound, options.upperBound, options.dir, options.limit);
            };
            case (null) {
                Debug.trap("missing sub-DB");
            };
        };
    };

    type ScanLimitOuterOptions = {outerSuperDB: SuperDB; outerKey: OuterSubDBKey; lowerBound: Text; upperBound: Text; dir: RBT.Direction; limit: Nat};
    
    /// Retrieve sub-DB entries by its outer key.
    public func scanLimitOuter(options: ScanLimitOuterOptions): async* RBT.ScanLimitResult<Text, AttributeValue> {
        let ?{canister = part; key = innerKey} = getInner({outerKey = options.outerKey; superDB = options.outerSuperDB}) else {
            Debug.trap("no sub-DB");
        };
        MyCycles.addPart<system>(options.outerSuperDB.dbOptions.partitionCycles);
        await part.scanLimitInner({innerKey; lowerBound = options.lowerBound; upperBound = options.upperBound; dir = options.dir; limit = options.limit});
    };

    type ScanLimitOuterPartitionKeyOptions = {outer: OuterPair; lowerBound: Text; upperBound: Text; dir: RBT.Direction; limit: Nat};
    
    public func scanLimitOuterPartitionKey(options: ScanLimitOuterPartitionKeyOptions): async* RBT.ScanLimitResult<Text, AttributeValue> {
        await options.outer.canister.scanLimitOuter({
            outerKey = options.outer.key;
            lowerBound = options.lowerBound;
            upperBound = options.upperBound;
            dir = options.dir;
            limit = options.limit;
        });
    };

    /// Retrieve a list of sub-DBs for a canister.
    ///
    /// Internal.
    public func scanSubDBs({superDB: SuperDB}): [(OuterSubDBKey, InnerPair)] {
        let iter = I.map<(OuterSubDBKey, {inner: InnerPair; /*var busy: ?OpsQueue.GUID*/}), (OuterSubDBKey, InnerPair)>(
            BTree.entries(superDB.locations),
            func(e: (OuterSubDBKey, {inner: InnerPair; /*var busy: ?OpsQueue.GUID*/})) { (e.0, e.1.inner) },
        );
        I.toArray(iter);
    };

    /// Canisters

    /// Get the array of partition canisters.
    public func getCanisters(dbIndex: DBIndex): [PartitionCanister] {
        StableBuffer.toArray(dbIndex.canisters);
    };

    /// Internal.
    public func createPartitionImpl(index: IndexCanister, dbIndex: DBIndex): async* Principal {
        MyCycles.addPart<system>(dbIndex.dbOptions.partitionCycles);
        let canister = await index.createPartition();
        let can2: PartitionCanister = actor(Principal.toText(canister));
        StableBuffer.add(dbIndex.canisters, can2);
        canister;
    };

    func comparePartition(x: PartitionCanister, y: PartitionCanister): {#equal; #greater; #less} {
        Principal.compare(Principal.fromActor(x), Principal.fromActor(y));
    };

    func compareLocs(x: Pair, y: Pair): {#equal; #greater; #less} {
        let c = comparePartition(x.canister, y.canister);
        if (c != #equal) {
            c;
        } else {
            Nat.compare(x.key, y.key);
        }
    };
};