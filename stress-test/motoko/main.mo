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

actor StressTest {
    // TODO: https://forum.dfinity.org/t/why-is-actor-class-constructor-not-shared/21424
    public shared func constructor(dbOptions: Nac.DBOptions): async Partition.Partition {
        MyCycles.addPart(dbOptions.partitionCycles);
        await Partition.Partition(dbOptions);
    };

    let dbOptions = {moveCap = #usedMemory 500_000; hardCap = null; partitionCycles = 10_000_000_000; constructor = constructor};

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
        threadNum: Nat;
        var referenceTree: ReferenceTree;
        var outerToGUID: OuterToGUID;
        var rng: Prng.Seiran128;
        index: Index.Index;
        guidGen: GUID.GUIDGenerator;
    };

    public func main() : async () {
        let seed : Nat64 = 0;
        var referenceTree: ReferenceTree = RBT.init();
        var outerToGUID: OuterToGUID = RBT.init();
        let rng = Prng.Seiran128();
        rng.init(seed);
        let guidGen = GUID.init(Array.tabulate<Nat8>(16, func _ = 0));

        MyCycles.addPart(dbOptions.partitionCycles);
        let index = await Index.Index(dbOptions);
        MyCycles.addPart(dbOptions.partitionCycles);
        await index.init();

        let nThreads = 3;
        let threads : [var ?(async())] = Array.init(nThreads, null);
        for (threadNum in threads.keys()) {
            threads[threadNum] := ?runThread({threadNum; var referenceTree; var outerToGUID; var rng; index; guidGen});
        };
        label F for (topt in threads.vals()) {
            let ?t = topt else {
                Debug.trap("programming error");
            };
            await t;
            break F;
        };

        let resultingTree = await* readResultingTree({referenceTree; outerToGUID; index});
        Debug.print("Reference tree size: " # debug_show(RBT.size(referenceTree)));
        Debug.print("Resulting tree size: " # debug_show(RBT.size(resultingTree)));
        let subtreeEqual = func(t1: RBT.Tree<Text, Nat>, t2: RBT.Tree<Text, Nat>): Bool {
            RBT.equalIgnoreDeleted(t1, t2, Text.equal, Nat.equal);
        };
        let equal = RBT.equalIgnoreDeleted<Nac.GUID, RBT.Tree<Text, Nat>>(referenceTree, resultingTree, Blob.equal, subtreeEqual);
        Debug.print("Equal? " # debug_show(equal));
    };

    func runThread(options: ThreadArguments) : async () {
        for (stepN in Iter.range(0, 100)) {
            // Debug.print("Step " # debug_show(options.threadNum) # "/" # Nat.toText(stepN));
            await* runStep(options);
        }
    };

    func runStep(options: ThreadArguments) : async* () {
        let random = options.rng.next();
        let variants = 4;
        if (random < Nat64.fromNat(rngBound / variants * 1)) {
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
                Debug.trap("programming error");
            };
            options.referenceTree := RBT.put(options.referenceTree, Blob.compare, guid, RBT.init<Text, Nat>());
            options.outerToGUID := RBT.put(options.outerToGUID, compareLocs, (part, subDBKey), guid);
        } else if (random < Nat64.fromNat(rngBound / variants * 2)) {
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
                };
                case (null) {};
            }
        } else if (random < Nat64.fromNat(rngBound / variants * 3)) {
            var v: ?(Partition.Partition, Nat) = null;
            let guid = GUID.nextGuid(options.guidGen);
            let sk = GUID.nextGuid(options.guidGen);
            let ?((part, outerKey), _) = randomItem(options) else {
                return;
            };
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
                        value = #int 0; // TODO
                    });
                } catch(e) {
                    Debug.print("repeat insert: " # Error.message(e));
                    continue R;
                };
                v := ?(part, outerKey);
                break R;
            };
            let ?(part3, outerKey3) = v else {
                Debug.trap("programming error");
            };
            let ?subtree = RBT.get(options.referenceTree, Blob.compare, guid) else {
                Debug.trap("subtree doesn't exist");
            };
            let subtree2 = RBT.put(subtree, Text.compare, debug_show(sk), 0);
            options.referenceTree := RBT.put(options.referenceTree, Blob.compare, guid, subtree2);
        } else {
            switch (randomItem(options)) {
                case (?((part, outerKey), sk)) {
                    let guid = GUID.nextGuid(options.guidGen);
                    label R loop {
                        try {
                            MyCycles.addPart(dbOptions.partitionCycles);
                            await part.delete({outerKey; sk});
                        } catch(e) {
                            Debug.print("repeat insert: " # Error.message(e));
                            continue R;
                        };
                        break R;
                    };
                    let ?subtree = RBT.get(options.referenceTree, Blob.compare, guid) else {
                        Debug.trap("subtree doesn't exist");
                    };
                    let subtree2 = RBT.delete(subtree, Text.compare, debug_show(sk));
                    options.referenceTree := RBT.put(options.referenceTree, Blob.compare, guid, subtree2);
                };
                case (null) {}
            }
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
            Debug.trap("programming error");
        };
        let n = Nat64.toNat(options.rng.next()) * RBT.size(db) / rngBound;
        let iter = RBT.entries(db);
        for (_ in Iter.range(0, n)) {
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
            for((_, (innerCanister, innerKey)) in (await part.scanSubDBs()).vals()) {
                let ?guid = RBT.get(outerToGUID, compareLocs, (innerCanister, innerKey)) else {
                    Debug.trap("readResultingTree: cannot get GUID");
                };
                var subtree = RBT.init<Text, Nat>();
                result := RBT.put(result, Blob.compare, guid, subtree);
                let scanned = await innerCanister.scanLimitInner({
                    innerKey; lowerBound = ""; upperBound = "\u{ffff}\u{ffff}\u{ffff}\u{ffff}"; dir = #fwd; limit = 1_000_000_000});
                for ((sk, v) in scanned.results.vals()) {
                    let #int v2 = v else {
                        Debug.trap("not #int");
                    };
                    subtree := RBT.put<Text, Nat>(subtree, Text.compare, sk, Int.abs(v2));
                };
            };
        };
        result;
    };
}