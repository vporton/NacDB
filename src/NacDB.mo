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
        var busy: Bool; // Forbid to move this entry to other canister.
                        // During the move it is true. Deletion in old canister and setting it to false happen in the same atomic action,
                        // so moving is also protected by this flag.
    };

    type MoveCap = { #numDBs: Nat; #usedMemory: Nat };

    type CreatingSubDB = {
        canister: PartitionCanister;
        subDBKey: SubDBKey
    };

    type SuperDB = {
        var nextKey: Nat;
        subDBs: BTree.BTree<SubDBKey, SubDB>;
        moveCap: MoveCap;

        var moving: ?{
            oldCanister: PartitionCanister;
            oldSuperDB: SuperDB;
            oldSubDBKey: SubDBKey;
            var newCanister: ?{ // null - not yet determined
                canister: PartitionCanister;
                var newSubDBKey: ?SubDBKey; // null - not yet determined
            };
        };
    };

    public type DBIndex = {
        var canisters: StableBuffer.StableBuffer<Principal>;
        var creatingSubDB: RBT.Tree<SubDBKey, CreatingSubDB>;
    };

    public type IndexCanister = actor {
        getCanisters(): async [PartitionCanister];
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
        isOverflowed() : async Bool;
        createSubDB({dbOptions: DBOptions}) : async Nat;
        releaseSubDB(subDBKey: SubDBKey) : async ();
        insert({subDBKey: SubDBKey; sk: SK; value: AttributeValue}) : async ();
        get: query (options: {subDBKey: SubDBKey; sk: SK}) -> async ?AttributeValue;
    };

    public func createDBIndex() : DBIndex {
        {
            var canisters = StableBuffer.init<Principal>();
            var creatingSubDB = RBT.init();
        }
    };

    public func createSuperDB(options: {
        moveCap: MoveCap;
    }) : SuperDB {
        {
            var nextKey = 0;
            subDBs = BTree.init<SubDBKey, SubDB>(null);
            moveCap = options.moveCap;
            var moving = null;
            var creatingSubDB = RBT.init();
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
        movingCallback: ?MovingCallback;
        maxSubDBsInCreating: Nat;
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

    public func finishMovingSpecifiedSubDB({index: IndexCanister; superDB: SuperDB; dbOptions: DBOptions}) : async* () {
        switch (superDB.moving) {
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
                        ignore BTree.delete(superDB.subDBs, Nat.compare, moving.oldSubDBKey); // FIXME: idempotent?
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
                        superDB.moving := null;
                    };
                    case (null) {};
                };
            };
            case (null) {};
        };
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
        indexCanister: IndexCanister;
        currentCanister: PartitionCanister;
        superDB: SuperDB;
        subDBKey: SubDBKey;
        sk: SK;
        value: AttributeValue;
    };

    // FIXME: not idempotent
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
            };
        };
    };

    public func createSubDB({superDB: SuperDB; dbOptions: DBOptions}) : Nat {
        let subDB : SubDB = {
            var data = RBT.init();
            hardCap = dbOptions.hardCap;
            movingCallback = dbOptions.movingCallback;
            var busy = false;
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

    public func creatingSubDBKeys(dbIndex: DBIndex) : [SubDBKey] {
        let iter = Iter.map(RBT.entries(dbIndex.creatingSubDB), func(e: (SubDBKey, CreatingSubDB)): SubDBKey {
            let (key, _) = e;
            key;
        });
        Iter.toArray(iter);
    };

    // It does not touch old items, so no locking.
    public func startCreatingSubDB({dbIndex: DBIndex; dbOptions: DBOptions}): async* (PartitionCanister, SubDBKey) {
        // Deque has no `size()`.
        if (RBT.size(dbIndex.creatingSubDB) >= dbOptions.maxSubDBsInCreating) {
            Debug.trap("queue full");
        };
        if (StableBuffer.size(dbIndex.canisters) == 0) {
            Debug.trap("no partition canisters");
        };
        let pk = StableBuffer.get(dbIndex.canisters, StableBuffer.size(dbIndex.canisters) - 1);
        let part: PartitionCanister = actor(Principal.toText(pk));
        // FIXME: The following is not idempotent.
        let subDBKey = await part.createSubDB({dbOptions}); // We don't need `busy = true`, because we didn't yet created "links" to it.
        dbIndex.creatingSubDB := RBT.put(dbIndex.creatingSubDB, Nat.compare, subDBKey, {
            canister = part; subDBKey = subDBKey;
        } : CreatingSubDB);
        (part, subDBKey);
    };

    public func finishCreatingSubDB(dbIndex: DBIndex) : async* () {
        for ((key, value) in RBT.entries(dbIndex.creatingSubDB)) {
            // await value.canister.releaseSubDB(key);
            dbIndex.creatingSubDB := RBT.delete(dbIndex.creatingSubDB, Nat.compare, key);
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