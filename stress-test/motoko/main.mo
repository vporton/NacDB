import Cycles "mo:base/ExperimentalCycles";
import Iter "mo:base/Iter";
import Debug "mo:base/Debug";
import Array "mo:base/Array";
import RBT "mo:stable-rbtree/StableRBTree";
import Prng "mo:prng";
import Nat64 "mo:base/Nat64";
import Nac "../../src/NacDB";
import Index "../../example/src/index/main";
import Partition "../../example/src/partition/main";
import Principal "mo:base/Principal";
import GUID "../../lib/GUID";
import MyCycles "../../lib/Cycles";
import Nat "mo:base/Nat";
import Error "mo:base/Error";
import Text "mo:base/Text";
import Blob "mo:base/Blob";
import Int "mo:base/Int";
import Buffer "mo:base/Buffer";
import Float "mo:base/Float";
import Int64 "mo:base/Int64";

actor StressTest {
    // TODO: https://forum.dfinity.org/t/why-is-actor-class-constructor-not-shared/21424
    public shared func constructor(dbOptions: Nac.DBOptions): async Partition.Partition {
        MyCycles.addPart(dbOptions.partitionCycles);
        await Partition.Partition(dbOptions);
    };

    let dbOptions = {moveCap = #usedMemory 300_000; hardCap = null; partitionCycles = 150_000_000_000; constructor = constructor};

    /// The tree considered already debugged for comparison to the being debugged one.
    type ReferenceTree = RBT.Tree<Nac.GUID, RBT.Tree<Text, Nat>>;

    type OuterToGUID = RBT.Tree<(Partition.Partition, Nac.OuterSubDBKey), Nac.GUID>;

    func comparePartition(x: Partition.Partition, y: Partition.Partition): {#equal; #greater; #less} {
        Principal.compare(Principal.fromActor(x), Principal.fromActor(y));
    };

    func compareLocs(x: (Partition.Partition, Nac.OuterSubDBKey), y: (Partition.Partition, Nac.OuterSubDBKey)): {#less; #equal; #greater} {
        let c = comparePartition(x.0, y.0);
        if (c != #equal) {
            c;
        } else {
            Nat.compare(x.1, y.1);
        }
    };

    let rngBound = 2**64;

    type ThreadArguments = {
        nSteps: Nat;
        var referenceTree: ReferenceTree;
        var outerToGUID: OuterToGUID;
        var rng: Prng.Seiran128;
        index: Index.Index;
        guidGen: GUID.GUIDGenerator;
        var recentOuter: Buffer.Buffer<Nac.OuterSubDBKey>;
        var recentSKs: Buffer.Buffer<Nac.SK>;
        var dbInserts: Nat;
        var dbDeletions: Nat;
        var eltInserts: Nat;
        var eltDeletions: Nat;
    };

    public func main() : async () {
        let nThreads = 1; //10;
        let nSteps = 610; // the mininum number leading to an error

        Debug.print("STARTING STRESS TEST: " # debug_show(nThreads) # " threads, each " # debug_show(nSteps) # " steps");

        let seed : Nat64 = 0;
        let rng = Prng.Seiran128();
        rng.init(seed);
        let guidGen = GUID.init(Array.tabulate<Nat8>(16, func _ = 0));

        MyCycles.addPart(dbOptions.partitionCycles);
        let index = await Index.Index(dbOptions);
        MyCycles.addPart(dbOptions.partitionCycles);
        await index.init();

        let threads : [var ?(async())] = Array.init(nThreads, null);
        let options: ThreadArguments = {
            nSteps;
            var referenceTree = RBT.init();
            var outerToGUID = RBT.init();
            var rng;
            index;
            guidGen;
            var recentOuter = Buffer.Buffer(0);
            var recentSKs = Buffer.Buffer(0);
            var dbInserts = 0;
            var dbDeletions = 0;
            var eltInserts = 0;
            var eltDeletions = 0;
        };
        for (threadNum in threads.keys()) {
            threads[threadNum] := ?runThread(options, threadNum);
        };
        for (topt in threads.vals()) {
            let ?t = topt else {
                Debug.trap("programming error: threads");
            };
            await t;
        };

        Debug.print("Number of partition canisters: " # debug_show(Array.size(await index.getCanisters())));
        Debug.print(
            "DB inserts: " # debug_show(options.dbInserts) #
            ", DB deletions: " # debug_show(options.dbDeletions) #
            ", Elt inserts: " # debug_show(options.eltInserts) #
            ", Elt deletions: " # debug_show(options.eltDeletions));

        let resultingTree = await* readResultingTree({referenceTree = options.referenceTree; outerToGUID = options.outerToGUID; index});
        Debug.print("Reference tree size: " # debug_show(RBT.size(options.referenceTree)));
        Debug.print("Resulting tree size: " # debug_show(RBT.size(resultingTree)));
        let subtreeEqual = func(t1: RBT.Tree<Text, Nat>, t2: RBT.Tree<Text, Nat>): Bool {
            RBT.equalIgnoreDeleted(t1, t2, Text.equal, Nat.equal);
        };
        let equal = RBT.equalIgnoreDeleted<Nac.GUID, RBT.Tree<Text, Nat>>(options.referenceTree, resultingTree, Blob.equal, subtreeEqual);
        Debug.print("Equal? " # debug_show(equal));
    };

    func runThread(options: ThreadArguments, threadNum: Nat) : async () {
        for (stepN in Iter.range(0, options.nSteps - 1)) {
            // Debug.print("Step " # debug_show(options.threadNum) # "/" # Nat.toText(stepN));
            await* runStep(options);
        }
    };

    func runStep(options: ThreadArguments) : async* () {
        let random = options.rng.next();
        let variants = 4;
        if (random < Nat64.fromNat(rngBound / variants * 1)) {
            Debug.print("createSuBDB start");
            var v: ?(Partition.Partition, Nat) = null;
            let guid = GUID.nextGuid(options.guidGen);
            label R loop {
                let {outer = (part, outerKey)} = try {
                    MyCycles.addPart(dbOptions.partitionCycles);
                    await options.index.createSubDB({guid; dbOptions; userData = debug_show(guid)});
                } catch(e) {
                    Debug.print("repeat createSubDB: " # Error.message(e));
                    continue R;
                };
                v := ?(part, outerKey);
                break R;
            };
            let ?(part, subDBKey) = v else {
                Debug.trap("programming error: createSubDB");
            };
            options.referenceTree := RBT.put(options.referenceTree, Blob.compare, guid, RBT.init<Text, Nat>());
            options.outerToGUID := RBT.put(options.outerToGUID, compareLocs, (part, subDBKey), guid);
            options.dbInserts += 1;
            Debug.print("createSuBDB finish");
        } else if (random < Nat64.fromNat(rngBound / variants * 2)) {
            Debug.print("deleteSuBDB start");
            switch (randomSubDB(options)) {
                case (?((part, outerKey), guid)) {
                    label R loop {
                        try {
                            MyCycles.addPart(dbOptions.partitionCycles);
                            await part.deleteSubDB({outerKey});
                        } catch(e) {
                            Debug.print("repeat deleteSubDB: " # Error.message(e));
                            continue R;
                        };
                        break R;
                    };
                    options.referenceTree := RBT.delete(options.referenceTree, Blob.compare, guid);
                    options.outerToGUID := RBT.delete(options.outerToGUID, compareLocs, (part, outerKey));
                };
                case (null) {};
            };
            options.dbDeletions += 1;
            Debug.print("deleteSuBDB finish");
        } else if (random < Nat64.fromNat(rngBound / variants * 3)) {
            Debug.print("insert start");
            var v: ?(Partition.Partition, Nat) = null;
            let guid = GUID.nextGuid(options.guidGen);
            let sk = GUID.nextGuid(options.guidGen);
            let ?((part, outerKey), _) = randomSubDB(options) else {
                return;
            };
            let randomValue = Nat64.toNat(options.rng.next());
            label R loop {
                let {outer = (part2, outerKey2)} = try {
                    MyCycles.addPart(dbOptions.partitionCycles);
                    await part.insert({
                        guid;
                        dbOptions;
                        indexCanister = options.index;
                        outerCanister = part;
                        outerKey;
                        sk = debug_show(sk);
                        value = #int randomValue;
                    });
                } catch(e) {
                    if (Text.endsWith(Error.message(e), #text " trapped explicitly: missing sub-DB")) {
                        // Debug.print("insert: missing sub-DB");
                        return; // Everything is OK, a not erroneous race condition.
                    };
                    Debug.print("repeat insert: " # Error.message(e));
                    continue R;
                };
                v := ?(part2, outerKey2);
                break R;
            };
            let ?(part3, outerKey3) = v else {
                Debug.trap("programming error: insert");
            };
            let ?guid2 = RBT.get(options.outerToGUID, compareLocs, (part3, outerKey3)) else {
                Debug.trap("programing error: level1 guid")
            };
            let ?subtree = RBT.get(options.referenceTree, Blob.compare, guid2) else { // FIXME: always null
                // Debug.print("subtree doesn't exist"); // Race condition: subtree was deleted after `randomSubDB()`.
                return; // Everything is OK, a not erroneous race condition.
            };
            let subtree2 = RBT.put(subtree, Text.compare, debug_show(sk), randomValue);
            options.referenceTree := RBT.put(options.referenceTree, Blob.compare, guid2, subtree2);
            options.eltInserts += 1;
            Debug.print("insert finish");
        } else {
            Debug.print("delete start");
            switch (randomItem(options)) {
                case (?((part, outerKey), sk)) {
                    label R loop {
                        try {
                            MyCycles.addPart(dbOptions.partitionCycles);
                            await part.delete({outerKey; sk});
                        } catch(e) {
                            Debug.print("repeat delete: " # Error.message(e));
                            continue R;
                        };
                        break R;
                    };
                    let ?guid2 = RBT.get(options.outerToGUID, compareLocs, (part, outerKey)) else {
                        Debug.trap("programing error: level1 guid")
                    };
                    let ?subtree = RBT.get(options.referenceTree, Blob.compare, guid2) else {
                        // Debug.print("subtree doesn't exist"); // Race condition: subtree was deleted after `randomItem()`.
                        return; // Everything is OK, a not erroneous race condition.
                    };
                    let subtree2 = RBT.delete(subtree, Text.compare, sk);
                    options.referenceTree := RBT.put(options.referenceTree, Blob.compare, guid2, subtree2);
                    // options.outerToGUID := RBT.put(options.outerToGUID, compareLocs, (part, outerKey), guid); // FIXME: Describe the race condition why we need it.
                };
                case (null) {}
            };
            options.eltDeletions += 1;
            Debug.print("delete finish");
        };
    };

    func randomSubDB(options: ThreadArguments): ?((Partition.Partition, Nac.OuterSubDBKey), Nac.GUID) {
        let n = Nat64.toNat(options.rng.next()) * RBT.size(options.referenceTree) / rngBound;
        let iter = RBT.entries(options.outerToGUID);
        for (_ in Iter.range(0, n)) {
            ignore iter.next();
        };
        iter.next();
    };

    // FIXME: With higher probability choose recent items
    // FIXME: Return GUID.
    func randomItem(options: ThreadArguments): ?((Partition.Partition, Nac.OuterSubDBKey), Text) {
        let ?(k, v) = randomSubDB(options) else {
            return null;
        };
        let ?db = RBT.get(options.referenceTree, Blob.compare, v) else {
            return null;
        };
        let n = Nat64.toNat(options.rng.next()) * RBT.size(db) / rngBound;
        let iter = RBT.entries(db);
        for (_ in Iter.range(0, n-1)) {
            ignore iter.next();
        };
        do ? {
            (k, iter.next()!.0);
        };
    };

    func readResultingTree({referenceTree: ReferenceTree; outerToGUID: OuterToGUID; index: Index.Index}): async* ReferenceTree {
        var result: ReferenceTree = RBT.init();
        let canisters = await index.getCanisters();
        for (part in canisters.vals()) {
            label L for((outerKey, (innerCanister, innerKey)) in (await part.scanSubDBs()).vals()) {
                let ?guid = RBT.get(outerToGUID, compareLocs, (part, outerKey)) else {
                    Debug.trap("cannot get GUID for " # debug_show(Principal.fromActor(part)) # " " # debug_show(outerKey)); // FIXME: Should be trap
                };
                var subtree = RBT.init<Text, Nat>();
                let scanned = await innerCanister.scanLimitInner({
                    innerKey; lowerBound = ""; upperBound = "\u{ffff}\u{ffff}\u{ffff}\u{ffff}"; dir = #fwd; limit = 1_000_000_000});
                for ((sk, v) in scanned.results.vals()) {
                    let #int v2 = v else {
                        Debug.trap("not #int");
                    };
                    subtree := RBT.put<Text, Nat>(subtree, Text.compare, sk, Int.abs(v2));
                };
                result := RBT.put(result, Blob.compare, guid, subtree);
            };
        };
        result;
    };
}