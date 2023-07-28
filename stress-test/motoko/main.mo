import Cycles "mo:base/ExperimentalCycles";
import Iter "mo:base/Iter";
import Debug "mo:base/Debug";
import Array "mo:base/Array";
import BTree "mo:btree/BTree";
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

actor StressTest {
    // TODO: https://forum.dfinity.org/t/why-is-actor-class-constructor-not-shared/21424
    public shared func constructor(dbOptions: Nac.DBOptions): async Partition.Partition {
        MyCycles.addPart(dbOptions.partitionCycles);
        await Partition.Partition(dbOptions);
    };

    let dbOptions = {moveCap = #usedMemory 500_000; hardCap = null; partitionCycles = 10_000_000_000; constructor = constructor};

    /// The tree considered already debugged for comparison to the being debugged one.
    type ReferenceTree = BTree.BTree<(Partition.Partition, Nac.OuterSubDBKey), BTree.BTree<Text, Nat>>;

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
        var rng: Prng.Seiran128;
        index: Index.Index;
        guidGen: GUID.GUIDGenerator;
    };

    public func main() : async () {
        let seed : Nat64 = 0;
        var referenceTree: ReferenceTree = BTree.init(null);
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
            threads[threadNum] := ?runThread({threadNum; var referenceTree; var rng; index; guidGen});
        };
        label F for (topt in threads.vals()) {
            let ?t = topt else {
                Debug.trap("programming error");
            };
            await t;
            break F;
        }
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
        if (random < Nat64.fromNat(rngBound / variants)) {
            var v: ?(Partition.Partition, Nat) = null;
            let guid = GUID.nextGuid(options.guidGen);
            label R loop {
                let {outer = (part, outerKey)} = try {
                    MyCycles.addPart(dbOptions.partitionCycles);
                    await options.index.createSubDB({guid; dbOptions; userData = ""});
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
            myAssert(
                "sub-DB already exists",
                not BTree.has(options.referenceTree, compareLocs, (part, subDBKey)));
            ignore BTree.insert(options.referenceTree, compareLocs, (part, subDBKey), BTree.init<Text, Nat>(null));
        } else if (random < Nat64.fromNat(rngBound / variants * 2)) {
            switch (randomSubDB(options)) {
                case (?((part, outerKey), _)) {
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
                    ignore BTree.delete(options.referenceTree, compareLocs, (part, outerKey));
                    myAssert(
                        "sub-DB wasn't deleted",
                        not BTree.has(options.referenceTree, compareLocs, (part, outerKey)));
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
                        value = #int 0;
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
            // myAssert(); // FIXME
            // let ?subtree = BTree.get(options.referenceTree, compareLocs, (part3, outerKey3)) else {
            //     // FIXME
            // };
            // ignore BTree.insert<Text, Nat>(subtree, Text.compare, debug_show(sk), 0);
        };
    };

    func myAssert(msg: Text, f: Bool) {
        if (not f) {
            Debug.print(msg);
        }
    };

    func randomSubDB(options: ThreadArguments): ?((Partition.Partition, Nac.OuterSubDBKey), BTree.BTree<Text, Nat>) {
        let n = Nat64.toNat(options.rng.next()) * BTree.size(options.referenceTree) / rngBound;
        let iter = BTree.entries(options.referenceTree);
        for (_ in Iter.range(0, n)) {
            ignore iter.next();
        };
        iter.next();
    };

    // FIXME: With higher probability choose recent items
    func randomItem(options: ThreadArguments): ?((Partition.Partition, Nac.OuterSubDBKey), Text) {
        let ?(k, v) = randomSubDB(options) else {
            return null;
        };
        let n = Nat64.toNat(options.rng.next()) * BTree.size(v) / rngBound;
        let iter = BTree.entries(v);
        for (_ in Iter.range(0, n)) {
            ignore iter.next();
        };
        do ? {
            (k, iter.next()!.0);
        };
    }
}