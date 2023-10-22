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
import SparseQueue "./SparseQueue";
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
        subDBKey: OuterSubDBKey;
        var needsMove: ?Bool;
        var insertingImplDone: Bool;
        var finishMovingSubDBDone: ?{
            newInnerPartition: InnerCanister;
            newInnerKey: OuterSubDBKey;
        };
    };

    public type InsertingItem2 = {
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
        var locations: BTree.BTree<OuterSubDBKey, {inner: (InnerCanister, InnerSubDBKey); /*var busy: ?SparseQueue.GUID*/}>;
    };

    public type DBIndex = {
        dbOptions: DBOptions;
        var canisters: StableBuffer.StableBuffer<PartitionCanister>;
        var creatingSubDB: SparseQueue.SparseQueue<CreatingSubDB>;
        var inserting: SparseQueue.SparseQueue<InsertingItem>;  // outer
        var inserting2: SparseQueue.SparseQueue<InsertingItem2>; // inner
        var moving: BTree.BTree<(OuterCanister, OuterSubDBKey), ()>;
        var blockDeleting: BTree.BTree<(OuterCanister, OuterSubDBKey), ()>;
    };

    public type IndexCanister = actor {
        // TODO: Can we make createPartitionImpl() a non-shared function?
        createPartitionImpl: shared() -> async Principal;
        createPartition: shared() -> async Principal;
        getCanisters: query () -> async [Principal];
        createSubDB: shared({guid: [Nat8]; userData: Text})
            -> async {inner: (Principal, InnerSubDBKey); outer: (Principal, OuterSubDBKey)};
        finishMovingSubDBImpl({
            guid: [Nat8];
            index: Principal;
            outerCanister: Principal;
            outerKey: OuterSubDBKey;
            oldInnerCanister: Principal;
            oldInnerKey: InnerSubDBKey;
        }) : async (Principal, InnerSubDBKey);
        insert({
            guid: [Nat8];
            indexCanister: Principal;
            outerCanister: Principal;
            outerKey: OuterSubDBKey;
            sk: SK;
            value: AttributeValue;
        }) : async {inner: (Principal, InnerSubDBKey); outer: (Principal, OuterSubDBKey)};
        delete({outerCanister: Principal; outerKey: OuterSubDBKey; sk: SK; guid: [Nat8]}): async ();
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
        isOverflowed: shared ({}) -> async Bool; // TODO: query
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
            var creatingSubDB = SparseQueue.init(dbOptions.createDBQueueLength, dbOptions.timeout);
            dbOptions;
            var inserting = SparseQueue.init(dbOptions.insertQueueLength, dbOptions.timeout);
            var inserting2 = SparseQueue.init(dbOptions.insertQueueLength, dbOptions.timeout);
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
                {inner = (canister, inner2); /*var busy: ?SparseQueue.GUID = null*/});
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
            {inner = (innerCanister, innerKey); /*var busy: ?SparseQueue.GUID = null*/});
    };

    /// This function makes no sense, because it would return the entire sub-DB from another canister.
    // public func getSubDBByOuter(superDB: SuperDB, outerKey: OuterSubDBKey) : ?SubDB {
    // };

    /// Called only if `isOverflowed`.
    /// FIXME: Error because of security consideration of calling from a partition canister.
    /// TODO: No need to present this in shared API.
    public func finishMovingSubDBImpl({
        guid: GUID; // TODO: superfluous argument
        dbIndex: DBIndex;
        index: IndexCanister; // TODO: needed?
        outerCanister: OuterCanister;
        outerKey: OuterSubDBKey;
        oldInnerCanister: InnerCanister;
        oldInnerKey: InnerSubDBKey;
    }) : async* (InnerCanister, InnerSubDBKey)
    {
        // TODO: would better have `inserting2` in `SuperDB` for less blocking?
        // TODO: No need for separate allocation of `InsertingItem2`, can put the value directly in `InsertingItem`.
        let inserting2: InsertingItem2 = {
            var newInnerCanister = null;
        };
        SparseQueue.add(dbIndex.inserting2, guid, inserting2);
        
        MyCycles.addPart(dbIndex.dbOptions.partitionCycles);
        let result = switch (await oldInnerCanister.rawGetSubDB({innerKey = oldInnerKey})) {
            case (?subDB) {
                let (canister, newCanister) = switch (inserting2.newInnerCanister) {
                    case (?newCanister) { (newCanister.canister, newCanister) };
                    case (null) {
                        MyCycles.addPart(dbIndex.dbOptions.partitionCycles);
                        let newCanister0 = await* createPartitionImpl(index, dbIndex);
                        let newCanister: PartitionCanister = actor(Principal.toText(newCanister0));
                        let s = {canister = newCanister; var innerKey: ?InnerSubDBKey = null};
                        inserting2.newInnerCanister := ?s;
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

        SparseQueue.delete(dbIndex.inserting2, guid);
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

    /// FIXME: Error because of security consideration of calling from a partition canister.
    // Sometimes traps "missing sub-DB".
    public func getByOuter(options: GetByOuterOptions) : async* ?AttributeValue {
        let ?(part, innerKey) = getInner(options.outerSuperDB, options.outerKey) else {
            Debug.trap("no entry");
        };
        MyCycles.addPart(options.outerSuperDB.dbOptions.partitionCycles);
        await part.getByInner({innerKey; sk = options.sk});
    };

    public type ExistsByInnerOptions = GetByInnerOptions;

    public func hasByInner(options: ExistsByInnerOptions) : Bool {
        getByInner(options) != null;
    };

    public type ExistsByOuterOptions = GetByOuterOptions;

    /// FIXME: Error because of security consideration of calling from a partition canister.
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

    public type GetUserDataOuterOptions = {superDB: SuperDB; outerKey: OuterSubDBKey};

    // TODO: Test this function
    /// FIXME: Error because of security consideration of calling from a partition canister.
    public func getSubDBUserDataOuter(options: GetUserDataOuterOptions) : async* ?Text {
        let ?(part, innerKey) = getInner(options.superDB, options.outerKey) else {
            Debug.trap("no sub-DB");
        };
        MyCycles.addPart(options.superDB.dbOptions.partitionCycles);
        await part.getSubDBUserDataInner({innerKey});
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

    /// FIXME: Error because of security consideration of calling from a partition canister.
    public func subDBSizeByOuter(options: SubDBSizeByOuterOptions): async* ?Nat {
        let ?(part, innerKey) = getInner(options.outerSuperDB, options.outerKey) else {
            Debug.trap("no sub-DB");
        };
        MyCycles.addPart(options.outerSuperDB.dbOptions.partitionCycles);
        await part.subDBSizeByInner({innerKey});
    };

    /// To be called in a partition where `innerSuperDB` resides.
    /// FIXME: Error because of security consideration of calling from a partition canister.
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

    // FIXME: Having both `outerCanister` and `outerCanister` in options is a bug.
    public type InsertOptions = {
        guid: GUID;
        indexCanister: Principal; // FIXME: Remove?
        dbIndex: DBIndex;
        outerCanister: Principal;
        // outerSuperDB: SuperDB;
        outerKey: OuterSubDBKey;
        sk: SK;
        value: AttributeValue;
    };

    /// There is no `insertByInner`, because inserting may need to move the sub-DB.
    /// FIXME: Error because of security consideration of calling from a partition canister.
    public func insert(options: InsertOptions)
        : async* {inner: (InnerCanister, InnerSubDBKey); outer: (OuterCanister, OuterSubDBKey)} // TODO: need to return this value?
    {
        let outer: OuterCanister = actor(Principal.toText(options.outerCanister));
        ignore BTree.insert(options.dbIndex.blockDeleting, compareLocs, (outer, options.outerKey), ());
        MyCycles.addPart(options.dbIndex.dbOptions.partitionCycles);
        let ?(oldInnerCanister, oldInnerKey) = await outer.getInner(options.outerKey) else {
            Debug.trap("missing sub-DB");
        };

        let inserting: InsertingItem = {
            part = options.outerCanister;
            subDBKey = options.outerKey;
            var needsMove = null;
            var insertingImplDone = false;
            var finishMovingSubDBDone = null;
        };
        SparseQueue.add(options.dbIndex.inserting, options.guid, inserting);

        if (not inserting.insertingImplDone) {
            let needsMove = switch(inserting.needsMove) {
                case(?needsMove) { needsMove };
                case(null) {
                    MyCycles.addPart(options.dbIndex.dbOptions.partitionCycles);
                    let needsMove = await oldInnerCanister.isOverflowed({});
                    inserting.needsMove := ?needsMove;
                    needsMove;
                };
            };
            MyCycles.addPart(options.dbIndex.dbOptions.partitionCycles);
            await oldInnerCanister.startInsertingImpl({
                guid = Blob.toArray(options.guid);
                indexCanister = options.indexCanister;
                outerCanister = options.outerCanister;
                outerKey = options.outerKey;
                sk = options.sk;
                value = options.value;
                innerKey = oldInnerKey;
                needsMove;
            });
            if (needsMove) {
                if (BTree.has(options.dbIndex.moving, compareLocs, (actor(Principal.toText(options.outerCanister)): OuterCanister, options.outerKey))) {
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
                        MyCycles.addPart(options.dbIndex.dbOptions.partitionCycles); // FIXME: here and in other places don't pass dbOptions
                        let needsMove = await oldInnerCanister.isOverflowed({});
                        inserting.needsMove := ?needsMove;
                        needsMove;
                    };
                };
                if (needsMove) {
                    MyCycles.addPart(options.dbIndex.dbOptions.partitionCycles);
                    let index: IndexCanister = actor(Principal.toText(options.indexCanister));
                    let (innerPartition, innerKey) = await* finishMovingSubDBImpl({
                        guid = options.guid;
                        dbIndex = options.dbIndex;
                        index = actor(Principal.toText(options.indexCanister));
                        oldInnerKey;
                        outerCanister = actor(Principal.toText(options.outerCanister));
                        outerKey = options.outerKey;
                        oldInnerCanister;
                    });
                    ignore BTree.delete(options.dbIndex.moving, compareLocs, (actor(Principal.toText(options.outerCanister)): OuterCanister, options.outerKey));
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

        SparseQueue.delete(options.dbIndex.inserting, options.guid);
        // releaseOuterKey(options.outerSuperDB, options.outerKey);
        ignore BTree.delete(options.dbIndex.blockDeleting, compareLocs, (outer, options.outerKey));

        {inner = (newInnerPartition, newInnerKey); outer = (outer, options.outerKey)};
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

    type DeleteOptions = {dbIndex: DBIndex; outerCanister: OuterCanister; outerKey: OuterSubDBKey; sk: SK; guid: GUID};
    
    /// idempotent
    public func delete(options: DeleteOptions): async* () {
        switch(await options.outerCanister.getInner(options.outerKey)) {
            case (?(innerCanister, innerKey)) {
                // FIXME: Do we need here to check `has()` before `insert()`?
                // Can we block here on inner key instead of outer one?
                if (BTree.has(options.dbIndex.blockDeleting, compareLocs, (options.outerCanister, options.outerKey))) {
                    Debug.trap("deleting is blocked");
                };
                MyCycles.addPart(options.dbIndex.dbOptions.partitionCycles);
                await innerCanister.deleteInner({innerKey; sk = options.sk});

            };
            case (null) {};
        };
        // releaseOuterKey(options.outerSuperDB, options.outerKey);
    };

    type DeleteDBOptions = {outerSuperDB: SuperDB; outerKey: OuterSubDBKey; guid: GUID};
    
    /// FIXME: Error because of security consideration of calling from a partition canister.
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

    // Creating sub-DB //

    /// It does not touch old items, so no locking.
    ///
    /// Pass a random GUID. Repeat the call with the same GUID, if the previous call failed.
    ///
    /// The "real" returned value is `outer`, but `inner` can be used for caching
    /// (on cache failure retrieve new `inner` using `outer`).
    ///
    /// In this version returned `PartitionCanister` for inner and outer always the same.
    public func createSubDB({guid: GUID; index: IndexCanister; dbIndex: DBIndex; userData: Text})
        : async* {inner: (InnerCanister, InnerSubDBKey); outer: (OuterCanister, OuterSubDBKey)}
    {
        let creating: CreatingSubDB = {var canister = null; var loc = null; userData};
        SparseQueue.add(dbIndex.creatingSubDB, guid, creating);
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
        let {inner; outer} = switch (creating.loc) {
            case (?loc) { loc };
            case (null) {
                MyCycles.addPart(dbIndex.dbOptions.partitionCycles);
                let {inner; outer} = await part3.rawInsertSubDBAndSetOuter([], null, creating.userData);
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
        : {inner: (InnerCanister, InnerSubDBKey); outer: (OuterCanister, OuterSubDBKey)}
    {
        ignore BTree.insert(outerSuperDB.locations, Nat.compare, outerKey,
            {inner = (part, innerKey); /*var busy: ?SparseQueue.GUID = null*/});
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
    
    /// FIXME: Error because of security consideration of calling from a partition canister.
    public func scanLimitOuter(options: ScanLimitOuterOptions): async* RBT.ScanLimitResult<Text, AttributeValue> {
        let ?(part, innerKey) = getInner(options.outerSuperDB, options.outerKey) else {
            Debug.trap("no sub-DB");
        };
        MyCycles.addPart(options.outerSuperDB.dbOptions.partitionCycles);
        await part.scanLimitInner({innerKey; lowerBound = options.lowerBound; upperBound = options.upperBound; dir = options.dir; limit = options.limit});
    };

    public func scanSubDBs({superDB: SuperDB}): [(OuterSubDBKey, (InnerCanister, InnerSubDBKey))] {
        let iter = Iter.map<(OuterSubDBKey, {inner: (InnerCanister, InnerSubDBKey); /*var busy: ?SparseQueue.GUID*/}), (OuterSubDBKey, (InnerCanister, InnerSubDBKey))>(
            BTree.entries(superDB.locations),
            func(e: (OuterSubDBKey, {inner: (InnerCanister, InnerSubDBKey); /*var busy: ?SparseQueue.GUID*/})) { (e.0, e.1.inner) },
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
        StableBuffer.add(dbIndex.canisters, can2); // TODO: too low level
        canister;
    };

    func comparePartition(x: PartitionCanister, y: PartitionCanister): {#equal; #greater; #less} {
        Principal.compare(Principal.fromActor(x), Principal.fromActor(y));
    };

    func compareLocs(x: (PartitionCanister, SubDBKey), y: (PartitionCanister, SubDBKey)): {#less; #equal; #greater} {
        let c = comparePartition(x.0, y.0);
        if (c != #equal) {
            c;
        } else {
            Nat.compare(x.1, y.1);
        }
    };
};