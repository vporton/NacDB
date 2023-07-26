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

actor StressTest {
    // TODO: https://forum.dfinity.org/t/why-is-actor-class-constructor-not-shared/21424
    public shared func constructor(dbOptions: Nac.DBOptions): async Partition.Partition {
        MyCycles.addPart();
        await Partition.Partition(dbOptions);
    };

    let dbOptions = {moveCap = #usedMemory 10_000; hardCap = null; partitionCycles = 1_000_000_000_000_000; constructor = constructor};

    /// The tree considered already debugged for comparison to the being debugged one.
    type ReferenceTree = BTree.BTree<(Principal, Nat), BTree.BTree<Text, Nat>>;

    func compareLocs(x: (Principal, Nat), y: (Principal, Nat)): {#less; #equal; #greater} {
        let c = Principal.compare(x.0, y.0);
        if (c != #equal) {
            c;
        } else {
            Nat.compare(x.1, y.1);
        }
    };

    let rngBound = 2**64;

    type ThreadArguments = {var referenceTree: ReferenceTree; var rng: Prng.Seiran128; index: Index.Index; guidGen: GUID.GUIDGenerator};

    public func main() : async () {
        let seed : Nat64 = 0;
        var referenceTree: ReferenceTree = BTree.init(null);
        let rng = Prng.Seiran128();
        rng.init(seed);
        let guidGen = GUID.init(Array.tabulate<Nat8>(16, func _ = 0));

        let dbOptions = {moveCap = #usedMemory 500_000; hardCap = ?1000; partitionCycles = 1_000_000_000_000_000; constructor = constructor};

        MyCycles.addPart();
        let index = await Index.Index(dbOptions);
        MyCycles.addPart();
        await index.init();

        let nThreads = 3;
        let threads : [var ?(async*())] = Array.init(nThreads, null);
        for (i in threads.keys()) {
            threads[i] := ?runThread({var referenceTree; var rng; index; guidGen});
        };     
        for (topt in threads.vals()) {
            let ?t = topt else {
                Debug.trap("programming error");
            };
            await* t;
        }
    };

    func runThread(options: ThreadArguments) : async* () {
        // for (_ in Iter.range(0, 333_333)) {
        for (stepN in Iter.range(0, 10)) {
            Debug.print("Step " # Nat.toText(stepN));
            await* runStep(options);
        }
    };

    func runStep(options: ThreadArguments) : async* () {
        let random = options.rng.next();
        let variants = 3;
        if (random < Nat64.fromNat(rngBound / variants)) {
            var v: ?(Principal, Nat) = null;
            let guid = GUID.nextGuid(options.guidGen);
            label R loop {
                let {outer = (part, outerKey)} = try {
                    MyCycles.addPart();
                    await options.index.createSubDB({guid; dbOptions; userData = ""});
                } catch(e) {
                    Debug.print("repeat createSubDB: " # Error.message(e));
                    continue R;
                };
                v := ?(Principal.fromActor(part), outerKey);
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
                    let partAct: Partition.Partition = actor(Principal.toText(part));
                    label R loop {
                        try {
                            MyCycles.addPart();
                            await partAct.deleteSubDB({outerKey});
                        } catch(e) {
                            Debug.print("repeat deleteSubDB: " # Error.message(e));
                            continue R;
                        }
                    };
                    myAssert(
                        "sub-DB doesn't exists",
                        BTree.has(options.referenceTree, compareLocs, (part, outerKey)));
                    ignore BTree.delete(options.referenceTree, compareLocs, (part, outerKey));
                };
                case (null) {};
            }
            // let {outer = (part2, subDBKey2)} = await part.insert({
            //     guid = GUID.nextGuid(guidGen);
            //     dbOptions;
            //     indexCanister = index;
            //     outerCanister = part;
            //     outerKey = subDBKey;
            //     sk = "name";
            //     value = #text name;
            // });
        }
    };

    func myAssert(msg: Text, f: Bool) {
        if (not f) {
            Debug.print(msg);
        }
    };

    func randomSubDB(options: ThreadArguments): ?((Principal, Nat), BTree.BTree<Text, Nat>) {
        let n = Nat64.toNat(options.rng.next()) * BTree.size(options.referenceTree) / rngBound;
        let iter = BTree.entries(options.referenceTree);
        for (_ in Iter.range(0, n)) {
            ignore iter.next();
        };
        iter.next();
    };
}