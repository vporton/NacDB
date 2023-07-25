import Debug "mo:base/Debug";
import ActorSpec "./utils/ActorSpec";
import Nac "../src/NacDB";
import Index "../example/src/index/main";
import Partition "../example/src/partition/main";
import Principal "mo:base/Principal";
import Array "mo:base/Array";
import SparseQueue "../lib/SparseQueue";
import GUID "../lib/GUID";

type Group = ActorSpec.Group;

let assertTrue = ActorSpec.assertTrue;
let describe = ActorSpec.describe;
let it = ActorSpec.it;
let skip = ActorSpec.skip;
let pending = ActorSpec.pending;
let run = ActorSpec.run;

// TODO: https://forum.dfinity.org/t/why-is-actor-class-constructor-not-shared/21424
shared func constructor(dbOptions: Nac.DBOptions): async Partition.Partition {
    await Partition.Partition(dbOptions);
};

let dbOptions = {moveCap = #usedMemory 500_000; hardCap = ?1000; partitionCycles = 300_000_000_000; constructor};

func createCanisters() : async* {index: Index.Index} {
    let index = await Index.Index(dbOptions);
    await index.init();
    {index};
};

let guidGen = GUID.init(Array.tabulate<Nat8>(16, func _ = 0));

func insertSubDB(index: Index.Index) : async* (Partition.Partition, Nac.OuterSubDBKey) {
    let {outer = (part, subDBKey)} = await index.createSubDB({guid = GUID.nextGuid(guidGen); dbOptions; userData = ""});
    (
        actor(Principal.toText(Principal.fromActor(part))),
        subDBKey,
    );
};

func createSubDB() : async* {index: Index.Index; part: Partition.Partition; subDBKey: Nac.OuterSubDBKey}
{
    let {index} = await* createCanisters();
    let {outer = (part, subDBKey)} = await index.createSubDB({guid = GUID.nextGuid(guidGen); dbOptions; userData = ""});
    {
        index = actor(Principal.toText(Principal.fromActor(index)));
        part = actor(Principal.toText(Principal.fromActor(part)));
        subDBKey;
    }
};

// TODO: Test passing userData
let success = run([
    describe("Unit Test of NacDB", [
        describe("Simple DB operations", [
            it("insert/get", do {
                let {index; part; subDBKey} = await* createSubDB();
                let name = "Dummy";
                ignore await part.insert({
                    guid = GUID.nextGuid(guidGen);
                    dbOptions;
                    indexCanister = index;
                    outerCanister = part;
                    outerKey = subDBKey;
                    sk = "name";
                    value = #text name;
                });
                let name2 = await part.getByOuter({subDBKey; sk = "name"});
                let has = await part.hasByOuter({subDBKey; sk = "name"});
                let has2 = await part.hasSubDBByOuter({subDBKey});
                ActorSpec.assertAllTrue([
                    name2 == ?(#text name),
                    has,
                    has2,
                ]);
            }),
            it("insert/get miss", do {
                let {index; part; subDBKey} = await* createSubDB();
                let name = "Dummy";
                ignore await part.insert({
                    guid = GUID.nextGuid(guidGen);
                    dbOptions;
                    indexCanister = index;
                    outerCanister = part;
                    outerKey = subDBKey;
                    sk = "name";
                    value = #text name;
                });
                let name2 = await part.getByOuter({subDBKey; sk = "namex"});
                let has = await part.hasByOuter({subDBKey; sk = "namex"});
                ActorSpec.assertAllTrue([
                    name2 == null,
                    not has,
                ]);
            }),
            it("hasSubDB miss", do {
                let {index; part; subDBKey} = await* createSubDB();
                let has2 = await part.hasByOuter({subDBKey; sk = "name"});
                ActorSpec.assertTrue(not has2);
            }),
            it("delete sub-DB", do {
                let {index; part; subDBKey} = await* createSubDB();
                await part.deleteSubDB({outerKey = subDBKey});
                let has2 = await part.hasSubDBByOuter({subDBKey});
                ActorSpec.assertTrue(not has2);
            }),
            it("elements count", do {
                let {index} = await* createCanisters();
                let (part1, subDBKey1) = await* insertSubDB(index);
                let (part2, subDBKey2) = await* insertSubDB(index);
                let (part3, subDBKey3) = await* insertSubDB(index);
                ignore await part3.insert({
                    guid = GUID.nextGuid(guidGen);
                    dbOptions;
                    indexCanister = index;
                    outerCanister = part3;
                    outerKey = subDBKey3;
                    sk = "name";
                    value = #text "xxx";
                });
                ignore await part3.insert({
                    guid = GUID.nextGuid(guidGen);
                    dbOptions;
                    indexCanister = index;
                    outerCanister = part3;
                    outerKey = subDBKey3;
                    sk = "name";
                    value = #text "xxx";
                }); // duplicate name
                ignore await part3.insert({
                    guid = GUID.nextGuid(guidGen);
                    dbOptions;
                    indexCanister = index;
                    outerCanister = part3;
                    outerKey = subDBKey3;
                    sk = "name2";
                    value = #text "yyy";
                });
                ActorSpec.assertAllTrue([
                    (await part3.subDBSizeByOuter({subDBKey = subDBKey3})) == ?2,
                    (await part3.superDBSize()) == 3,
                ]);
            }),
            it("create a new partition canister", do {
                let dbOptions2 = {moveCap = #usedMemory 500_000; hardCap = ?1000; constructor; partitionCycles = 300_000_000_000};
                let index = await Index.Index(dbOptions2);
                await index.init();
                ignore await index.createSubDB({guid = GUID.nextGuid(guidGen); dbOptions = dbOptions2; userData = ""});
                ignore await index.createSubDB({guid = GUID.nextGuid(guidGen); dbOptions = dbOptions2; userData = ""});
                let {outer = (part, subDBKey)} = await index.createSubDB({guid = GUID.nextGuid(guidGen); dbOptions = dbOptions2; userData = ""});

                ActorSpec.assertAllTrue([
                    // part == (await index.getCanisters())[1] // TODO
                ]);
            }),
            // Cannot test it because without DFX Prim.rts_heap_size() is always zero. Will test it during stress testing.
            // it("move overflowed DB", do {
            //     let index = await Index.Index(?(#usedMemory 50000));
            //     await index.init();
            //     var address: ?(Nac.PartitionCanister, Nac.SubDBKey) = null;
            //     let creatingId = await index.startCreatingSubDBDetailed({moveCap = #numDBs 2; movingCallback = MyTest.movingCallback; hardCap = ?1000});
            //     address := ?(await index.finishCreatingSubDB({dbOptions = dbOptions2; index; creatingId}));
            //     label cycle loop {
            //         let ?(part, subDBKey) = address else {
            //             Debug.trap("can't destructure address");
            //         };
            //         let canisters = await index.getCanisters();
            //         Debug.print(debug_show(Array.size(canisters)));
            //         if (Array.size(canisters) == 2) {
            //             break cycle;
            //         };
            //     };

            //     let ?(part, subDBKey) = address else {
            //         Debug.trap("can't destructure address");
            //     };
            //     let canisters = await index.getCanisters();
            //     ActorSpec.assertAllTrue([
            //         part == canisters[1],
            //         (await MyTest.getCounter()) == 1,
            //         part == (await index.getCanisters())[1],
            //     ]);
            // }),
            it("remove loosers", do {
                let dbOptions2 = {moveCap = #usedMemory 500_000; hardCap = ?2; constructor; partitionCycles = 300_000_000_000};
                let index = await Index.Index(dbOptions2);
                await index.init();
                let {outer = (part, subDBKey)} = await index.createSubDB({guid = GUID.nextGuid(guidGen); dbOptions = dbOptions2; userData = ""});
                ignore await part.insert({
                    guid = GUID.nextGuid(guidGen);
                    dbOptions = dbOptions2;
                    indexCanister = index;
                    outerCanister = part;
                    outerKey = subDBKey;
                    sk = "A";
                    value = #text "xxx";
                });
                ignore await part.insert({
                    guid = GUID.nextGuid(guidGen);
                    dbOptions = dbOptions2;
                    indexCanister = index;
                    outerCanister = part;
                    outerKey = subDBKey;
                    sk = "B";
                    value = #text "xxx";
                });
                ignore await part.insert({
                    guid = GUID.nextGuid(guidGen);
                    dbOptions = dbOptions2;
                    indexCanister = index;
                    outerCanister = part;
                    outerKey = subDBKey;
                    sk = "C";
                    value = #text "xxx";
                });

                ActorSpec.assertAllTrue([
                    not (await part.hasByOuter({subDBKey; sk = "A"})),
                    await part.hasByOuter({subDBKey; sk = "B"}),
                    await part.hasByOuter({subDBKey; sk = "C"}),
                ]);
            }),
            it("iters", do {
                let {index} = await* createCanisters();
                let (part, subDBKey) = await* insertSubDB(index);
                ignore await part.insert({guid = GUID.nextGuid(guidGen); dbOptions; indexCanister = index; outerCanister = part; outerKey = subDBKey; sk = "A"; value = #text "xxx"});
                ignore await part.insert({guid = GUID.nextGuid(guidGen); dbOptions; indexCanister = index; outerCanister = part; outerKey = subDBKey; sk = "B"; value = #text "yyy"}); // duplicate name

                let scan1 = await part.scanLimitOuter({outerKey = subDBKey; lowerBound = ""; upperBound = "z"; dir = #fwd; limit = 2});
                let scan2 = await part.scanLimitOuter({outerKey = subDBKey; lowerBound = ""; upperBound = "z"; dir = #fwd; limit = 3}); // limit above length
                let scan3 = await part.scanLimitOuter({outerKey = subDBKey; lowerBound = ""; upperBound = "z"; dir = #fwd; limit = 1}); // partial
                let ?nextKey = scan3.nextKey else {
                    Debug.trap("no next key");
                };
                let scan4 = await part.scanLimitOuter({outerKey = subDBKey; lowerBound = nextKey; upperBound = "z"; dir = #fwd; limit = 1});
                let scan5 = await part.scanLimitOuter({outerKey = subDBKey; lowerBound = ""; upperBound = "z"; dir = #bwd; limit = 2});

                ActorSpec.assertAllTrue([
                    scan1.results == [("A", #text "xxx"), ("B", #text "yyy")],
                    scan2.results == [("A", #text "xxx"), ("B", #text "yyy")],
                    scan4.results == [("B", #text "yyy")],
                    scan5.results == [("B", #text "yyy"), ("A", #text "xxx")],
                ]);
            }),
        ]),
    ]),
]);

if (success == false) {
  Debug.trap("Tests failed");
}
