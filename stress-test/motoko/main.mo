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
import GUID "../../src/GUID";
import MyCycles "../../src/Cycles";
import Nat "mo:base/Nat";
import Error "mo:base/Error";
import Text "mo:base/Text";
import Blob "mo:base/Blob";
import Int "mo:base/Int";
import Buffer "mo:base/Buffer";
import Float "mo:base/Float";
import Int64 "mo:base/Int64";
import BTree "mo:stableheapbtreemap/BTree";

actor StressTest {
    let dbOptions = {
        moveCap = #usedMemory 300_000;
        partitionCycles = 28_000_000_000;
        createDBQueueLength = 60;
        insertQueueLength = 60;
    };

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
        var recentOuter: Buffer.Buffer<(Nac.OuterCanister, Nac.OuterSubDBKey)>;
        var recentSKs: Buffer.Buffer<((Nac.OuterCanister, Nac.OuterSubDBKey), Nac.SK)>;
        var dbInserts: Nat;
        var dbDeletions: Nat;
        var eltInserts: Nat;
        var eltDeletions: Nat;
    };

    public func main() : async () {
        let nThreads = 3;
        let nSteps = 300;

        Debug.print("STARTING STRESS TEST: " # debug_show(nThreads) # " threads, each " # debug_show(nSteps) # " steps");

        let seed : Nat64 = 0;
        let rng = Prng.Seiran128();
        rng.init(seed);
        let guidGen = GUID.init(Array.tabulate<Nat8>(16, func _ = 0));

        MyCycles.addPart(dbOptions.partitionCycles);
        let index = await Index.Index();
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
            if (RBT.equalIgnoreDeleted(t1, t2, Text.equal, Nat.equal)) {
                true;
            } else {
                Debug.print(debug_show(Iter.toArray(RBT.entries(t1))) # "/" # debug_show(Iter.toArray(RBT.entries(t2))));
                false;
            }
        };
        let equal = RBT.equalIgnoreDeleted<Nac.GUID, RBT.Tree<Text, Nat>>(options.referenceTree, resultingTree, Blob.equal, subtreeEqual);
        Debug.print("Equal? " # debug_show(equal));

        var brokenOuterCount = 0;
        // for (c in (await index.getCanisters()).vals()) {
        //     for ((outerKey, (innerCanister, innerKey)) in (await c.scanSubDBs()).vals()) {
        //         if (not (await innerCanister.hasSubDBByInner({innerKey}))) {
        //             brokenOuterCount += 1;
        //         }
        //     }
        // };
        let partitions = await index.getCanisters();
        let nThreads2 = Array.size(partitions);
        let threads2 : [var ?(async())] = Array.init(nThreads2, null);
        let runThread2 = func(outerPart: Nac.OuterCanister) : async () {
            for ((outerKey, (innerCanister, innerKey)) in (await outerPart.scanSubDBs()).vals()) {
                let innerCanister2: Nac.InnerCanister = actor(Principal.toText(innerCanister));
                if (not (await innerCanister2.hasSubDBByInner({innerKey}))) {
                    brokenOuterCount += 1;
                }
            }
        };
        for (threadNum in threads2.keys()) {
            let partitions2: Nac.PartitionCanister = actor(Principal.toText(partitions[threadNum]));
            threads2[threadNum] := ?runThread2(partitions2);
        };
        for (topt in threads2.vals()) {
            let ?t = topt else {
                Debug.trap("programming error: threads2");
            };
            await t;
        };
        Debug.print("Broken outer links: " # debug_show(brokenOuterCount));
    };

    // func runThread2(outerPart: Nac.OuterCanister) : async () {
    //     for ((outerKey, (innerCanister, innerKey)) in (await outerPart.scanSubDBs()).vals()) {
    //         if (not (await innerCanister.hasSubDBByInner({innerKey}))) {
    //             brokenOuterCount += 1;
    //         }
    //     }
    // };

    func runThread(options: ThreadArguments, threadNum: Nat) : async () {
        for (stepN in Iter.range(0, options.nSteps - 1)) {
            // Debug.print("Step " # debug_show(options.threadNum) # "/" # Nat.toText(stepN));
            await* runStep(options);
        }
    };

    func runStep(options: ThreadArguments) : async* () {
        let random = options.rng.next();
        let variants = 4+2;
        if (random < Nat64.fromNat(rngBound / variants * (1+1))) { // two times greater probability
            options.dbInserts += 1;
            var v: ?(Principal, Nat) = null;
            let guid = GUID.nextGuid(options.guidGen);
            label R loop {
                let {outer = (part, outerKey)} = try {
                    MyCycles.addPart(dbOptions.partitionCycles);
                    await options.index.createSubDB(Blob.toArray(guid), {userData = debug_show(guid); hardCap = null});
                } catch(e) {
                    continue R;
                };
                v := ?(part, outerKey);
                break R;
            };
            let ?(part0, subDBKey) = v else {
                Debug.trap("programming error: createSubDB");
            };
            let part: Partition.Partition = actor(Principal.toText(part0));
            options.referenceTree := RBT.put(options.referenceTree, Blob.compare, guid, RBT.init<Text, Nat>());
            options.outerToGUID := RBT.put(options.outerToGUID, compareLocs, (part, subDBKey), guid);
            options.recentOuter.add((part, subDBKey));
        } else if (random < Nat64.fromNat(rngBound / variants * (2+1))) {
            options.dbDeletions += 1;
            switch (randomSubDB(options)) {
                case (?(part, outerKey)) {
                    let guid = GUID.nextGuid(options.guidGen);
                    label R loop {
                        try {
                            MyCycles.addPart(dbOptions.partitionCycles);
                            await options.index.deleteSubDB(Blob.toArray(guid), {
                                outerKey;
                                outerCanister = Principal.fromActor(part);
                            });
                        } catch(e) {
                            // Debug.print("repeat deleteSubDB: " # Error.message(e));
                            continue R;
                        };
                        break R;
                    };
                    switch (RBT.get(options.outerToGUID, compareLocs, (part, outerKey))) {
                        case (?guid) {
                            options.referenceTree := RBT.delete(options.referenceTree, Blob.compare, guid);
                        };
                        case (null) {};
                    };
                    // options.outerToGUID := RBT.delete(options.outerToGUID, compareLocs, (part, outerKey)); // Uncomment?
                    options.recentOuter.add((part, outerKey));
                };
                case (null) {};
            };
        } else if (random < Nat64.fromNat(rngBound / variants * (3+2))) { // two times greater probability
            options.eltInserts += 1;
            var v: ?(Partition.Partition, Nat) = null;
            let guid = GUID.nextGuid(options.guidGen);
            let sk = GUID.nextGuid(options.guidGen);
            let ?(part, outerKey) = randomSubDB(options) else {
                return;
            };
            let randomValue = Nat64.toNat(options.rng.next());
            label R loop {
                let res = try {
                    MyCycles.addPart(dbOptions.partitionCycles);
                    await options.index.insert(Blob.toArray(guid), {
                        dbOptions;
                        outerCanister = Principal.fromActor(part);
                        outerKey;
                        sk = debug_show(sk);
                        value = #int randomValue;
                        hardCap = null;
                    });
                } catch(e) {
                    // Debug.print("repeat insert: " # Error.message(e));
                    continue R;
                };
                switch (res) {
                    case (#ok res) {
                        let {outer = (part2, outerKey2)} = res;
                        let part3: Nac.PartitionCanister = actor(Principal.toText(part2));
                        v := ?(part3, outerKey2);
                    };
                    case (#err "missing sub-DB") { // Everything is OK, a not erroneous race condition.
                        return;
                    };
                    case (#err _) {
                        Debug.trap("unexpected insert error");
                    };
                };
                break R;
            };
            let ?(part3, outerKey3) = v else {
                Debug.trap("programming error: insert");
            };
            let ?guid2 = RBT.get(options.outerToGUID, compareLocs, (part3, outerKey3)) else {
                return; // It was meanwhile deleted by another thread.
            };
            let ?subtree = RBT.get(options.referenceTree, Blob.compare, guid2) else {
                // Debug.print("subtree doesn't exist"); // Race condition: subtree was deleted after `randomSubDB()`.
                return; // Everything is OK, a not erroneous race condition.
            };
            let subtree2 = RBT.put(subtree, Text.compare, debug_show(sk), randomValue);
            options.referenceTree := RBT.put(options.referenceTree, Blob.compare, guid2, subtree2);
            options.recentOuter.add((part3, outerKey3));
            options.recentSKs.add(((part3, outerKey3), debug_show(sk)));
        } else {
            options.eltDeletions += 1;
            let guid = GUID.nextGuid(options.guidGen);
            switch (randomItem(options)) {
                case (?((part, outerKey), sk)) {
                    label R loop {
                        try {
                            MyCycles.addPart(dbOptions.partitionCycles);
                            await options.index.delete(Blob.toArray(guid), {outerCanister = Principal.fromActor(part); outerKey; sk});
                        } catch(e) {
                            // Debug.print("repeat delete: " # Error.message(e));
                            continue R;
                        };
                        break R;
                    };
                    let ?guid2 = RBT.get(options.outerToGUID, compareLocs, (part, outerKey)) else {
                        return; // It was meanwhile deleted by another thread.
                    };
                    let ?subtree = RBT.get(options.referenceTree, Blob.compare, guid2) else {
                        // Debug.print("subtree doesn't exist"); // Race condition: subtree was deleted after `randomItem()`.
                        return; // Everything is OK, a not erroneous race condition.
                    };
                    let subtree2 = RBT.delete(subtree, Text.compare, sk);
                    options.referenceTree := RBT.put(options.referenceTree, Blob.compare, guid2, subtree2);
                    options.recentOuter.add((part, outerKey));
                    options.recentSKs.add(((part, outerKey), sk));
                };
                case (null) {}
            };
        };
    };

    func randomBufferElementPreferringNearEnd<T>(rng: Prng.Seiran128, buf: Buffer.Buffer<T>): ?T {
        if (buf.size() == 0) {
            return null;
        };
        // sum_{i=0,n-1}(1/2^i) = 2 - 2^(1-n)
        let max = Int.abs(2 - 1/2**(buf.size() - 1));
        let r = Float.fromInt(Nat64.toNat(rng.next())) * Float.fromInt(max) / 2**64;
        var i = 0;
        var sum = 0.0;
        // Debug.print("RAND: " # debug_show(r));
        label it loop {
            sum := sum + (2.0)**(-Float.fromInt(i));
            // Debug.print("SUM : " # debug_show(sum));
            if (sum < r) {
                i += 1;
                continue it;
            };
            if (i >= buf.size()) {
                return null;
            };
            let e = ?buf.remove(i);
            // Put the value on the top:
            let ?e2 = e else {
                return null;
            };
            buf.add(e2);
            return e;
        };
        Debug.trap("programming error");
    };

    func randomSubDB(options: ThreadArguments): ?(Partition.Partition, Nac.OuterSubDBKey) {
        // For stress testing, choose either...
        if (options.rng.next() < 2**63) {
            if (options.rng.next() < 2**63) { // a "gather many" sub-DB
                return switch (RBT.entries(options.outerToGUID).next()) {
                    case (?res) { ?res.0 };
                    case (null) { null };
                };
            };
            // ... a random value in the tree
            let n = Nat64.toNat(options.rng.next()) * RBT.size(options.referenceTree) / rngBound;
            let iter = RBT.entries(options.outerToGUID);
            for (_ in Iter.range(0, n-1)) {
                ignore iter.next();
            };
            switch (iter.next()) {
                case (?res) { ?res.0 };
                case (null) { null };
            };
        } else {
            // ... or a recently used value.
            randomBufferElementPreferringNearEnd(options.rng, options.recentOuter);
        };
    };

    func randomItem(options: ThreadArguments): ?((Partition.Partition, Nac.OuterSubDBKey), Text) {
        let ?(k, v) = randomSubDB(options) else {
            return null;
        };
        let ?guid = RBT.get(options.outerToGUID, compareLocs, (k, v)) else {
            return null;
        };
        let ?db = RBT.get<Nac.GUID, RBT.Tree<Text, Nat>>(options.referenceTree, Blob.compare, guid) else {
            return null;
        };
        // For stress testing, choose either...
        if (options.rng.next() < 2**63) {
            // ... a random value in the tree
            let n = Nat64.toNat(options.rng.next()) * RBT.size(db) / rngBound;
            let iter = RBT.entries(db);
            for (_ in Iter.range(0, n-1)) {
                ignore iter.next();
            };
            do ? {
                ((k, v), iter.next()!.0);
            };
        } else {
            // ... or a recently used value.
            randomBufferElementPreferringNearEnd(options.rng, options.recentSKs);
        };
    };

    func readResultingTree({referenceTree: ReferenceTree; outerToGUID: OuterToGUID; index: Index.Index}): async* ReferenceTree {
        var result: ReferenceTree = RBT.init();
        let canisters = await index.getCanisters();
        for (part in canisters.vals()) {
            let part2: Nac.PartitionCanister = actor(Principal.toText(part));
            label L for ((outerKey, (innerCanister, innerKey)) in (await part2.scanSubDBs()).vals()) {
                let ?guid = RBT.get<(Partition.Partition, Nac.OuterSubDBKey), Nac.GUID>(outerToGUID, compareLocs, (part2, outerKey)) else {
                    Debug.trap("cannot get GUID for " # debug_show(Principal.fromActor(part2)) # " " # debug_show(outerKey));
                };
                var subtree = RBT.init<Text, Nat>();
                let innerCanister2: Nac.PartitionCanister = actor(Principal.toText(innerCanister));
                let scanned = await innerCanister2.scanLimitInner({
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