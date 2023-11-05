import Result "mo:base/Result";
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
import OpsQueue "./OpsQueue";
import MyCycles "./Cycles";
import Blob "mo:base/Blob";
import Array "mo:base/Array";
import Time "mo:base/Time";

module {
    public type GUID = Blob;

    public type SubDBKey = Nat;

    /// The key under which a sub-DB stored in a canister.
    public type InnerSubDBKey = SubDBKey;

    /// Constant (regarding moving a sub-DB to another canister) key mapped to `InnerSubDBKey`.
    public type OuterSubDBKey = SubDBKey;

    public type SK = Text;

    public type AttributeKey = Text;

    // TODO: I've commented out some types due to https://github.com/dfinity/motoko/issues/4213
    public type AttributeValuePrimitive = {#text : Text; #int : Int; #bool : Bool; #float : Float};
    // public type AttributeValueBlob = {#blob : Blob};
    public type AttributeValueTuple = {#tuple : [AttributeValuePrimitive]};
    public type AttributeValueArray = {#arrayText : [Text]; #arrayInt : [Int]; #arrayBool : [Bool]; #arrayFloat : [Float]};
    // public type AttributeValueRBTreeValue = AttributeValuePrimitive or /*AttributeValueBlob or*/ AttributeValueTuple or AttributeValueArray;
    // public type AttributeValueRBTree = {#tree : RBT.Tree<Text, AttributeValueRBTreeValue>};
    public type AttributeValue = AttributeValuePrimitive or /*AttributeValueBlob or*/ AttributeValueTuple or AttributeValueArray /*or AttributeValueRBTree*/;

    public type SubDB = {
        var map: BTree.BTree<SK, AttributeValue>;
        var userData: Text; // useful to have a back reference to "locator" of our sub-DB in another database
    };

    public type MoveCap = { #usedMemory: Nat };

    public type CreatingSubDB = {
        var canister: ?PartitionCanister; // Immediately after creation of sub-DB, this is both inner and outer.
        var loc: ?{inner: (InnerCanister, InnerSubDBKey); outer: (OuterCanister, InnerSubDBKey)};
        userData: Text;
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

    public type SuperDB = {
        dbOptions: DBOptions;
        var nextInnerKey: Nat;
        var nextOuterKey: Nat;
        subDBs: BTree.BTree<InnerSubDBKey, SubDB>;
        /// `inner` of this `RBT.Tree` is constant,
        /// even when the sub-DB to which it points moves to a different canister.
        var locations: BTree.BTree<OuterSubDBKey, {inner: (InnerCanister, InnerSubDBKey); /*var busy: ?OpsQueue.GUID*/}>;
    };

    public type DBIndex = {
        dbOptions: DBOptions;
        var canisters: StableBuffer.StableBuffer<PartitionCanister>;
        var creatingSubDB: OpsQueue.OpsQueue<CreatingSubDB, CreateSubDBResult>;
        var inserting: OpsQueue.OpsQueue<InsertingItem, InsertResult>;  // outer
        var deleting: OpsQueue.OpsQueue<DeletingItem, ()>;
        var moving: BTree.BTree<(OuterCanister, OuterSubDBKey), ()>;
        var blockDeleting: BTree.BTree<(OuterCanister, OuterSubDBKey), ()>; // used to prevent insertion after DB deletion
    };

    public type IndexCanister = actor {
        createPartition: shared() -> async Principal;
        getCanisters: query () -> async [Principal];
        createSubDB: shared({guid: [Nat8]; userData: Text})
            -> async {inner: (Principal, InnerSubDBKey); outer: (Principal, OuterSubDBKey)};
        insert(guid: [Nat8], {
            indexCanister: Principal;
            outerCanister: Principal;
            outerKey: OuterSubDBKey;
            sk: SK;
            value: AttributeValue;
        }) : async Result.Result<{inner: (Principal, InnerSubDBKey); outer: (Principal, OuterSubDBKey)}, Text>;
        delete(guid: [Nat8], {outerCanister: Principal; outerKey: OuterSubDBKey; sk: SK}): async ();
    };

    // TODO: arguments as {...}, not (...).
    public type PartitionCanister = actor {
        // Mandatory //
        rawGetSubDB: query ({innerKey: InnerSubDBKey}) -> async ?{map: [(SK, AttributeValue)]; userData: Text};
        rawDeleteSubDB: ({innerKey: InnerSubDBKey}) -> async ();
        rawInsertSubDB(map: [(SK, AttributeValue)], inner: ?InnerSubDBKey, userData: Text)
            : async {inner: InnerSubDBKey};
        rawInsertSubDBAndSetOuter(
            map: [(SK, AttributeValue)],
            keys: ?{
                inner: InnerSubDBKey;
                outer: OuterSubDBKey;
            },
            userData: Text,
        )
            : async {inner: InnerSubDBKey; outer: OuterSubDBKey};
        getInner: query (outerKey: OuterSubDBKey) -> async ?(InnerCanister, InnerSubDBKey);
        isOverflowed: shared ({}) -> async Bool; // TODO: If I change it to query, it does not work. Why?
        putLocation(outerKey: OuterSubDBKey, innerCanister: Principal, newInnerSubDBKey: InnerSubDBKey) : async ();
        // In the current version two partition canister are always the same.
        createOuter(part: Principal, outerKey: OuterSubDBKey, innerKey: InnerSubDBKey)
            : async {inner: (Principal, InnerSubDBKey); outer: (Principal, OuterSubDBKey)};
        startInsertingImpl(options: {
            guid: [Nat8];
            indexCanister: Principal;
            outerCanister: Principal;
            outerKey: OuterSubDBKey;
            sk: SK;
            value: AttributeValue;
            innerKey: InnerSubDBKey;
            needsMove: Bool;
        }): async ();

        // Optional //

        superDBSize: query () -> async Nat;
        deleteSubDB({outerKey: OuterSubDBKey; guid: [Nat8]}) : async ();
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
        scanSubDBs: query() -> async [(OuterSubDBKey, (Principal, InnerSubDBKey))];
        getSubDBUserDataOuter: shared (options: {outerKey: OuterSubDBKey}) -> async ?Text;
        getSubDBUserDataInner: shared (options: {innerKey: InnerSubDBKey}) -> async ?Text;
    };

    public type InnerCanister = PartitionCanister;

    public type OuterCanister = PartitionCanister;

    public func createDBIndex(dbOptions: DBOptions) : DBIndex {
        {
            var canisters = StableBuffer.init<PartitionCanister>();
            var creatingSubDB = OpsQueue.init(dbOptions.createDBQueueLength);
            dbOptions;
            var inserting = OpsQueue.init(dbOptions.insertQueueLength);
            var deleting = OpsQueue.init(dbOptions.insertQueueLength);
            var moving = BTree.init(null);
            var blockDeleting = BTree.init(null);
        };
    };

    public func createSuperDB(dbOptions: DBOptions) : SuperDB {
        {
            dbOptions;
            var nextInnerKey = 0;
            var nextOuterKey = 0;
            subDBs = BTree.init<InnerSubDBKey, SubDB>(null);
            var locations = BTree.init(null);
        };
    };

    public type DBOptions = {
        hardCap: ?Nat;
        moveCap: MoveCap;
        partitionCycles: Nat;
        timeout: Time.Time;
        createDBQueueLength: Nat;
        insertQueueLength: Nat;
    };

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
    public func rawInsertSubDB(
        superDB: SuperDB,
        map: [(SK, AttributeValue)],
        inner: ?InnerSubDBKey,
        userData: Text,
    ) : {inner: InnerSubDBKey}
    {
        let key = switch (inner) {
            case (?key) { key };
            case (null) {
                let key = superDB.nextInnerKey;
                superDB.nextInnerKey += 1;
                key;
            };
        };                    
        let subDB : SubDB = {
            var map = BTree.fromArray(8, Text.compare, map);
            var userData = userData;
        };
        ignore BTree.insert(superDB.subDBs, Nat.compare, key, subDB);
        {inner = key};
    };

    public func rawDeleteSubDB(superDB: SuperDB, innerKey: InnerSubDBKey) : () {
        ignore BTree.delete(superDB.subDBs, Nat.compare, innerKey);
    };

    /// Use only if sure that outer and inner canisters coincide.
    public func rawInsertSubDBAndSetOuter(
        superDB: SuperDB,
        canister: InnerCanister,
        map: [(SK, AttributeValue)],
        keys: ?{
            inner: InnerSubDBKey;
            outer: OuterSubDBKey;
        },
        userData: Text,
    ) : {outer: OuterSubDBKey; inner: InnerSubDBKey}
    {
        let {inner = inner2} = rawInsertSubDB(superDB, map, do ? {keys!.inner}, userData);
        if (keys == null) {
            ignore BTree.insert(superDB.locations, Nat.compare, superDB.nextOuterKey,
                {inner = (canister, inner2); /*var busy: ?OpsQueue.GUID = null*/});
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

    public func getInner(superDB: SuperDB, outerKey: OuterSubDBKey) : ?(InnerCanister, InnerSubDBKey) {
        do ? {
            BTree.get(superDB.locations, Nat.compare, outerKey)!.inner;
        }
    };

    public func getSubDBByInner(superDB: SuperDB, innerKey: InnerSubDBKey) : ?SubDB {
        BTree.get(superDB.subDBs, Nat.compare, innerKey);
    };

    public func putLocation(outerSuperDB: SuperDB, outerKey: OuterSubDBKey, innerCanister: InnerCanister, innerKey: InnerSubDBKey) {
        ignore BTree.insert(outerSuperDB.locations, Nat.compare, outerKey,
            {inner = (innerCanister, innerKey); /*var busy: ?OpsQueue.GUID = null*/});
    };

    /// This function makes no sense, because it would return the entire sub-DB from another canister.
    // public func getSubDBByOuter(superDB: SuperDB, outerKey: OuterSubDBKey) : ?SubDB {
    // };

    /// Called only if `isOverflowed`.
    public func finishMovingSubDBImpl({
        inserting: InsertingItem;
        dbIndex: DBIndex;
        index: IndexCanister; // TODO: needed?
        outerCanister: OuterCanister;
        outerKey: OuterSubDBKey;
        oldInnerCanister: InnerCanister;
        oldInnerKey: InnerSubDBKey;
    }) : async* (InnerCanister, InnerSubDBKey)
    {
        MyCycles.addPart(dbIndex.dbOptions.partitionCycles);
        let result = switch (await oldInnerCanister.rawGetSubDB({innerKey = oldInnerKey})) {
            case (?subDB) {
                let (canister, newCanister) = switch (inserting.newInnerCanister) {
                    case (?newCanister) { (newCanister.canister, newCanister) };
                    case (null) {
                        MyCycles.addPart(dbIndex.dbOptions.partitionCycles);
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
                        if (BTree.has(dbIndex.moving, compareLocs, (outerCanister, outerKey))) {
                            Debug.trap("DB is scaling");
                        };
                        MyCycles.addPart(dbIndex.dbOptions.partitionCycles);
                        let {inner} = await canister.rawInsertSubDB(subDB.map, null, subDB.userData);
                        newCanister.innerKey := ?inner;
                        inner;
                    }
                };

                // There was `isOverflowed`, change the outer.
                MyCycles.addPart(dbIndex.dbOptions.partitionCycles);
                await outerCanister.putLocation(outerKey, Principal.fromActor(canister), newInnerSubDBKey);
                MyCycles.addPart(dbIndex.dbOptions.partitionCycles);
                await oldInnerCanister.rawDeleteSubDB({innerKey = oldInnerKey});

                (canister, newInnerSubDBKey);
            };
            case (null) {
                Debug.trap("no sub-DB");
            };
        };

        result;
    };

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

    func removeLoosers({superDB: SuperDB; subDB: SubDB}) {
        switch (superDB.dbOptions.hardCap) {
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

    /// Sometimes traps "missing sub-DB".
    public func getByOuter(options: GetByOuterOptions) : async* ?AttributeValue {
        let ?(part, innerKey) = getInner(options.outerSuperDB, options.outerKey) else {
            Debug.trap("no entry");
        };
        MyCycles.addPart(options.outerSuperDB.dbOptions.partitionCycles);
        await part.getByInner({innerKey; sk = options.sk});
    };

    public type GetByOuterPartitionKeyOptions = {outer: OuterCanister; outerKey: OuterSubDBKey; sk: SK};

    // TODO: shared method
    public func getByOuterPartitionKey(options: GetByOuterPartitionKeyOptions, dbOptions: DBOptions) : async* ?AttributeValue {
        MyCycles.addPart(dbOptions.partitionCycles);
        await options.outer.getByOuter({outerKey = options.outerKey; sk = options.sk});
    };

    public type ExistsByInnerOptions = GetByInnerOptions;

    public func hasByInner(options: ExistsByInnerOptions) : Bool {
        getByInner(options) != null;
    };

    public type ExistsByOuterOptions = GetByOuterOptions;

    public func hasByOuter(options: ExistsByOuterOptions) : async* Bool {
        (await* getByOuter(options)) != null;
    };

    public type HasByOuterPartitionKeyOptions = GetByOuterPartitionKeyOptions;

    // TODO: shared method
    public func hasByOuterPartitionKey(options: GetByOuterPartitionKeyOptions) : async Bool {
        await options.outer.hasByOuter({outerKey = options.outerKey; sk = options.sk});
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

    public type GetUserDataOuterOptions = {superDB: SuperDB; outerKey: OuterSubDBKey};

    // TODO: Test this function
    public func getSubDBUserDataOuter(options: GetUserDataOuterOptions) : async* ?Text {
        let ?(part, innerKey) = getInner(options.superDB, options.outerKey) else {
            Debug.trap("no sub-DB");
        };
        MyCycles.addPart(options.superDB.dbOptions.partitionCycles);
        await part.getSubDBUserDataInner({innerKey});
    };

    public type GetUserDataPartitionKeyOptions = {outer: OuterCanister; outerKey: OuterSubDBKey};

    // TODO: shared method
    public func getSubDBUserDataOuterPartitionKey(options: GetUserDataPartitionKeyOptions, dbOptions: DBOptions) : async* ?Text {
        MyCycles.addPart(dbOptions.partitionCycles);
        await options.outer.getSubDBUserDataOuter({outerKey = options.outerKey});
    };

    public type GetUserDataInnerOptions = {superDB: SuperDB; subDBKey: InnerSubDBKey};

    public func getSubDBUserDataInner(options: GetUserDataInnerOptions) : ?Text {
        do ? { BTree.get(options.superDB.subDBs, Nat.compare, options.subDBKey)!.userData };
    };

    public func superDBSize(superDB: SuperDB): Nat = BTree.size(superDB.subDBs);

    public type SubDBSizeByInnerOptions = {superDB: SuperDB; innerKey: InnerSubDBKey};

    public func subDBSizeByInner(options: SubDBSizeByInnerOptions): ?Nat {
        do ? {
            BTree.size(getSubDBByInner(options.superDB, options.innerKey)!.map);
        }
    };

    public type SubDBSizeByOuterOptions = {outerSuperDB: SuperDB; outerKey: OuterSubDBKey};

    public func subDBSizeByOuter(options: SubDBSizeByOuterOptions): async* ?Nat {
        let ?(part, innerKey) = getInner(options.outerSuperDB, options.outerKey) else {
            Debug.trap("no sub-DB");
        };
        MyCycles.addPart(options.outerSuperDB.dbOptions.partitionCycles);
        await part.subDBSizeByInner({innerKey});
    };

    public type SubDBSizeByOuterPartitionKeyOptions = {outer: OuterCanister; outerKey: OuterSubDBKey};

    // TODO: shared method
    public func subDBSizeByOuterPartitionKey(options: SubDBSizeByOuterPartitionKeyOptions, dbOptions: DBOptions): async* ?Nat {
        MyCycles.addPart(dbOptions.partitionCycles);
        await options.outer.subDBSizeByOuter({outerKey = options.outerKey});
    };

    /// To be called in a partition where `innerSuperDB` resides.
    public func startInsertingImpl(options: {
        guid: GUID;
        indexCanister: IndexCanister;
        outerCanister: OuterCanister;
        outerKey: OuterSubDBKey;
        sk: SK;
        value: AttributeValue;
        innerSuperDB: SuperDB;
        innerKey: InnerSubDBKey;
        needsMove: Bool;
    }) : async* () {
        switch (getSubDBByInner(options.innerSuperDB, options.innerKey)) {
            case (?subDB) {
                ignore BTree.insert(subDB.map, Text.compare, options.sk, options.value);
                removeLoosers({superDB = options.innerSuperDB; subDB});
            };
            case (null) {
                Debug.trap("missing sub-DB");
            };
        };
    };

    public type InsertOptions = {
        indexCanister: Principal; // TODO: Remove?
        dbIndex: DBIndex;
        outerCanister: Principal;
        outerKey: OuterSubDBKey;
        sk: SK;
        value: AttributeValue;
    };

    public type InsertResult = Result.Result<{inner: (InnerCanister, InnerSubDBKey); outer: (OuterCanister, OuterSubDBKey)}, Text>; // TODO: need to return this value?

    /// There is no `insertByInner`, because inserting may need to move the sub-DB.
    /// TODO: Other functions should also return `Result`?
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

                OpsQueue.add(options.dbIndex.inserting, guid, inserting);
                if (BTree.has(options.dbIndex.blockDeleting, compareLocs, (outer, options.outerKey))) {
                    Debug.trap("blocking deleting");
                };
                ignore BTree.insert(options.dbIndex.blockDeleting, compareLocs, (outer, options.outerKey), ());
                inserting;
            };
        };

        await* insertFinishByQueue(guid, inserting);
    };

    public func insertFinish(guid: GUID, dbIndex: DBIndex) : async* ?InsertResult {
        OpsQueue.result(dbIndex.inserting, guid);
    };

    func insertFinishByQueue(guid: GUID, inserting: InsertingItem) : async* InsertResult {
        let outer: OuterCanister = actor(Principal.toText(inserting.options.outerCanister)); // TODO: duplicate operation

        MyCycles.addPart(inserting.options.dbIndex.dbOptions.partitionCycles);
        let ?(oldInnerCanister, oldInnerKey) = await outer.getInner(inserting.options.outerKey) else {
            ignore BTree.delete(inserting.options.dbIndex.blockDeleting, compareLocs, (outer, inserting.options.outerKey));
            let result = #err "missing sub-DB";
            OpsQueue.answer(inserting.options.dbIndex.inserting, guid, result);
            return result;
        };

        if (not inserting.insertingImplDone) {
            let needsMove = switch(inserting.needsMove) {
                case(?needsMove) { needsMove };
                case(null) {
                    MyCycles.addPart(inserting.options.dbIndex.dbOptions.partitionCycles);
                    let needsMove = await oldInnerCanister.isOverflowed({});
                    inserting.needsMove := ?needsMove;
                    needsMove;
                };
            };
            MyCycles.addPart(inserting.options.dbIndex.dbOptions.partitionCycles);
            await oldInnerCanister.startInsertingImpl({
                guid = Blob.toArray(guid);
                indexCanister = inserting.options.indexCanister;
                outerCanister = inserting.options.outerCanister;
                outerKey = inserting.options.outerKey;
                sk = inserting.options.sk;
                value = inserting.options.value;
                innerKey = oldInnerKey;
                needsMove;
            });
            if (needsMove) {
                if (BTree.has(inserting.options.dbIndex.moving, compareLocs, (actor(Principal.toText(inserting.options.outerCanister)): OuterCanister, inserting.options.outerKey))) {
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
                        MyCycles.addPart(inserting.options.dbIndex.dbOptions.partitionCycles);
                        let needsMove = await oldInnerCanister.isOverflowed({});
                        inserting.needsMove := ?needsMove;
                        needsMove;
                    };
                };
                if (needsMove) {
                    MyCycles.addPart(inserting.options.dbIndex.dbOptions.partitionCycles);
                    let index: IndexCanister = actor(Principal.toText(inserting.options.indexCanister));
                    let (innerPartition, innerKey) = await* finishMovingSubDBImpl({
                        inserting;
                        dbIndex = inserting.options.dbIndex;
                        index = actor(Principal.toText(inserting.options.indexCanister));
                        oldInnerKey;
                        outerCanister = actor(Principal.toText(inserting.options.outerCanister));
                        outerKey = inserting.options.outerKey;
                        oldInnerCanister;
                    });
                    ignore BTree.delete(inserting.options.dbIndex.moving, compareLocs, (actor(Principal.toText(inserting.options.outerCanister)): OuterCanister, inserting.options.outerKey));
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

        ignore BTree.delete(inserting.options.dbIndex.blockDeleting, compareLocs, (outer, inserting.options.outerKey));

        #ok {inner = (newInnerPartition, newInnerKey); outer = (outer, inserting.options.outerKey)};
    };

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
    
    /// idempotent
    public func delete(guid: GUID, options: DeleteOptions): async* () {
        ignore OpsQueue.whilePending<DeletingItem, ()>(options.dbIndex.deleting, func(guid: GUID, elt: DeletingItem): async* () {
            OpsQueue.answer(
                options.dbIndex.deleting,
                guid,
                await* deleteFinishByQueue(guid, elt));
        });

        let deleting = switch (OpsQueue.get(options.dbIndex.deleting, guid)) {
            case (?deleting) { deleting };
            case null {
                let result = { options };
                OpsQueue.add(options.dbIndex.deleting, guid, result);
                if (BTree.has(options.dbIndex.blockDeleting, compareLocs, (options.outerCanister, options.outerKey))) {
                    Debug.trap("deleting is blocked");
                };
                ignore BTree.insert(options.dbIndex.blockDeleting, compareLocs, (options.outerCanister, options.outerKey), ());
                result;
            };
        };

        await* deleteFinishByQueue(guid, deleting);
    };

    type DeletingItem = {
        options: DeleteOptions;
    };

    // FIXME: Here and in other places, also finish previous deleting operations.
    public func deleteFinish(guid: GUID, dbIndex: DBIndex) : async* ?() {
        OpsQueue.result(dbIndex.deleting, guid);
    };

    func deleteFinishByQueue(guid: GUID, deleting: DeletingItem) : async* () {
        switch(await deleting.options.outerCanister.getInner(deleting.options.outerKey)) {
            case (?(innerCanister, innerKey)) {
                // Can we block here on inner key instead of outer one?
                MyCycles.addPart(deleting.options.dbIndex.dbOptions.partitionCycles);
                await innerCanister.deleteInner({innerKey; sk = deleting.options.sk});
            };
            case (null) {};
        };

        ignore BTree.delete(deleting.options.dbIndex.blockDeleting, compareLocs, (deleting.options.outerCanister, deleting.options.outerKey));
        OpsQueue.answer(deleting.options.dbIndex.deleting, guid, ());
    };

    type DeleteDBOptions = {outerSuperDB: SuperDB; outerKey: OuterSubDBKey; guid: GUID};
    
    public func deleteSubDB(options: DeleteDBOptions): async* () {
        switch(getInner(options.outerSuperDB, options.outerKey)) {
            case (?(innerCanister, innerKey)) {
                MyCycles.addPart(options.outerSuperDB.dbOptions.partitionCycles);
                await innerCanister.deleteSubDBInner({innerKey});
            };
            case (null) {};
        };
        ignore BTree.delete(options.outerSuperDB.locations, Nat.compare, options.outerKey);
    };

    public func deleteSubDBInner({superDB: SuperDB; innerKey: InnerSubDBKey}) : async* () {
        ignore BTree.delete(superDB.subDBs, Nat.compare, innerKey);
    };

    type DeleteDBPartitionKeyOptions = {outer: OuterCanister; outerKey: OuterSubDBKey; guid: GUID};

    // TODO: shared method
    public func deleteSubDBPartitionKey(options: DeleteDBPartitionKeyOptions): async* () {
        await options.outer.deleteSubDB({guid = Blob.toArray(options.guid); outerKey = options.outerKey});
    };

    // Creating sub-DB //

    type CreateSubDBResult = {inner: (InnerCanister, InnerSubDBKey); outer: (OuterCanister, OuterSubDBKey)};

    /// It does not touch old items, so no locking.
    ///
    /// Pass a random GUID. Repeat the call with the same GUID, if the previous call failed.
    ///
    /// The "real" returned value is `outer`, but `inner` can be used for caching
    /// (on cache failure retrieve new `inner` using `outer`).
    ///
    /// In this version returned `PartitionCanister` for inner and outer always the same.
    public func createSubDB({guid: GUID; index: IndexCanister; dbIndex: DBIndex; userData: Text})
        : async* CreateSubDBResult
    {
        let creating: CreatingSubDB = {var canister = null; var loc = null; userData};
        OpsQueue.add(dbIndex.creatingSubDB, guid, creating);
        let part3: PartitionCanister = switch (creating.canister) { // both inner and outer
            case (?part) { part };
            case (null) {
                let canisters = StableBuffer.toArray(dbIndex.canisters);
                let part = canisters[canisters.size() - 1];
                MyCycles.addPart(dbIndex.dbOptions.partitionCycles);
                let part2 = if (await part.isOverflowed({})) {
                    let part20 = await* createPartitionImpl(index, dbIndex);
                    let part2: PartitionCanister = actor(Principal.toText(part20));
                    creating.canister := ?part;
                    part2;
                } else {
                    let {inner; outer} = switch (creating.loc) {
                        case (?loc) { loc };
                        case (null) {
                            MyCycles.addPart(dbIndex.dbOptions.partitionCycles);
                            let {inner; outer} = await part.rawInsertSubDBAndSetOuter([], null, creating.userData);
                            creating.loc := ?{inner = (part, inner); outer = (part, outer)};
                            {inner = (part, inner); outer = (part, outer)};
                        };
                    };
                    part;
                };
            };
        };
        let result = switch (creating.loc) {
            case (?loc) { loc };
            case (null) {
                MyCycles.addPart(dbIndex.dbOptions.partitionCycles);
                let {inner; outer} = await part3.rawInsertSubDBAndSetOuter([], null, creating.userData);
                creating.loc := ?{inner = (part3, inner); outer = (part3, outer)};
                {inner = (part3, inner); outer = (part3, outer)};
            };
        };
        OpsQueue.answer(dbIndex.creatingSubDB, guid, result);
        result;
    };

    /// In the current version two partition canister are always the same.
    ///
    /// `superDB` should reside in `part`.
    public func createOuter(outerSuperDB: SuperDB, part: PartitionCanister, outerKey: OuterSubDBKey, innerKey: InnerSubDBKey)
        : {inner: (InnerCanister, InnerSubDBKey); outer: (OuterCanister, OuterSubDBKey)}
    {
        ignore BTree.insert(outerSuperDB.locations, Nat.compare, outerKey,
            {inner = (part, innerKey); /*var busy: ?OpsQueue.GUID = null*/});
        {inner = (part, innerKey); outer = (part, outerKey)};
    };

    // Scanning/enumerating //

    type IterInnerOptions = {innerSuperDB: SuperDB; innerKey: InnerSubDBKey};
    
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
    
    public func entriesInner(options: EntriesInnerOptions) : I.Iter<(Text, AttributeValue)> {
        iterByInner({innerSuperDB = options.innerSuperDB; innerKey = options.innerKey; dir = #fwd});
    };

    // Impossible to implement.
    // type EntriesOuterOptions = {outerSuperDB: SuperDB; outerKey: OuterSubDBKey};
    
    // public func entriesOuter(options: EntriesOptions) : I.Iter<(Text, AttributeValue)> {
    // };

    type EntriesRevInnerOptions = {innerSuperDB: SuperDB; innerKey: InnerSubDBKey};
    
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
    
    public func scanLimitOuter(options: ScanLimitOuterOptions): async* RBT.ScanLimitResult<Text, AttributeValue> {
        let ?(part, innerKey) = getInner(options.outerSuperDB, options.outerKey) else {
            Debug.trap("no sub-DB");
        };
        MyCycles.addPart(options.outerSuperDB.dbOptions.partitionCycles);
        await part.scanLimitInner({innerKey; lowerBound = options.lowerBound; upperBound = options.upperBound; dir = options.dir; limit = options.limit});
    };

    type ScanLimitOuterPartitionKeyOptions = {outer: OuterCanister; outerKey: OuterSubDBKey; lowerBound: Text; upperBound: Text; dir: RBT.Direction; limit: Nat};
    
    public func scanLimitOuterPartitionKey(options: ScanLimitOuterPartitionKeyOptions): async* RBT.ScanLimitResult<Text, AttributeValue> {
        await options.outer.scanLimitOuter({
            outerKey = options.outerKey;
            lowerBound = options.lowerBound;
            upperBound = options.upperBound;
            dir = options.dir;
            limit = options.limit;
        });
    };

    public func scanSubDBs({superDB: SuperDB}): [(OuterSubDBKey, (InnerCanister, InnerSubDBKey))] {
        let iter = Iter.map<(OuterSubDBKey, {inner: (InnerCanister, InnerSubDBKey); /*var busy: ?OpsQueue.GUID*/}), (OuterSubDBKey, (InnerCanister, InnerSubDBKey))>(
            BTree.entries(superDB.locations),
            func(e: (OuterSubDBKey, {inner: (InnerCanister, InnerSubDBKey); /*var busy: ?OpsQueue.GUID*/})) { (e.0, e.1.inner) },
        );
        Iter.toArray(iter);
    };

    /// Canisters

    public func getCanisters(dbIndex: DBIndex): [PartitionCanister] {
        StableBuffer.toArray(dbIndex.canisters);
    };

    public func createPartitionImpl(index: IndexCanister, dbIndex: DBIndex): async* Principal {
        MyCycles.addPart(dbIndex.dbOptions.partitionCycles);
        let canister = await index.createPartition();
        let can2: PartitionCanister = actor(Principal.toText(canister));
        StableBuffer.add(dbIndex.canisters, can2);
        canister;
    };

    func comparePartition(x: PartitionCanister, y: PartitionCanister): {#equal; #greater; #less} {
        Principal.compare(Principal.fromActor(x), Principal.fromActor(y));
    };

    func compareLocs(x: (PartitionCanister, SubDBKey), y: (PartitionCanister, SubDBKey)): {#equal; #greater; #less} {
        let c = comparePartition(x.0, y.0);
        if (c != #equal) {
            c;
        } else {
            Nat.compare(x.1, y.1);
        }
    };
};