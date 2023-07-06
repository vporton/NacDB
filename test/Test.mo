import Debug "mo:base/Debug";
import ActorSpec "./utils/ActorSpec";
import Nac "../src/NacDB";
import Index "../example/src/index/main";
import Partition "../example/src/partition/main";
import Principal "mo:base/Principal";
import Array "mo:base/Array";
import SparseQueue "../lib/SparseQueue";

type Group = ActorSpec.Group;

let assertTrue = ActorSpec.assertTrue;
let describe = ActorSpec.describe;
let it = ActorSpec.it;
let skip = ActorSpec.skip;
let pending = ActorSpec.pending;
let run = ActorSpec.run;

// TODO: Not good to duplicate in more than two places:
let moveCap = #usedMemory 500_000;
let dbOptions = {moveCap; movingCallback = null; hardCap = ?1000};

let moveCap2 = #numDBs 2;
let dbOptions2 = {moveCap = moveCap2; movingCallback = null; hardCap = ?1000};

func createCanisters() : async* {index: Index.Index} {
    let index = await Index.Index(null);
    await index.init(null); // TODO: `movingCallback`
    {index};
};

func insertSubDB(index: Index.Index) : async* (Partition.Partition, Nac.SubDBKey) {
    let creatingId = await index.startCreatingSubDB({dbOptions});
    let (part, subDBKey) = await index.finishCreatingSubDB({dbOptions; index; creatingId});
    (
        actor(Principal.toText(Principal.fromActor(part))),
        subDBKey,
    );
};

func createSubDB() : async* {index: Index.Index; part: Partition.Partition; subDBKey: Nac.SubDBKey}
{
    let {index} = await* createCanisters();
    let insertId = await index.startCreatingSubDB({dbOptions});
    let (part, subDBKey) = await index.finishCreatingSubDB({dbOptions; index; creatingId = insertId});
    {
        index = actor(Principal.toText(Principal.fromActor(index)));
        part = actor(Principal.toText(Principal.fromActor(part)));
        subDBKey;
    }
};

let success = run([
    describe("Unit Test of NacDB", [
        describe("Simple DB operations", [
            it("insert/get", do {
                let {index; part; subDBKey} = await* createSubDB();
                let name = "Dummy";
                let insertId = await part.startInserting({subDBKey = subDBKey; sk = "name"; value = #text name});
                ignore await part.finishInserting({dbOptions; index; insertId});
                let name2 = await part.get({subDBKey; sk = "name"});
                let has = await part.has({subDBKey; sk = "name"});
                let has2 = await part.hasSubDB({subDBKey});
                ActorSpec.assertAllTrue([
                    name2 == ?(#text name),
                    has,
                    has2,
                ]);
            }),
            it("insert/get miss", do {
                let {index; part; subDBKey} = await* createSubDB();
                let name = "Dummy";
                let insertId = await part.startInserting({subDBKey = subDBKey; sk = "name"; value = #text name});
                ignore await part.finishInserting({dbOptions; index; insertId});
                let name2 = await part.get({subDBKey; sk = "namex"});
                let has = await part.has({subDBKey; sk = "namex"});
                ActorSpec.assertAllTrue([
                    name2 == null,
                    not has,
                ]);
            }),
            it("hasSubDB miss", do {
                let {index; part; subDBKey} = await* createSubDB();
                let has2 = await part.has({subDBKey; sk = "name"});
                ActorSpec.assertTrue(not has2);
            }),
            it("elements count", do {
                let {index} = await* createCanisters();
                let (part1, subDBKey1) = await* insertSubDB(index);
                let (part2, subDBKey2) = await* insertSubDB(index);
                let (part3, subDBKey3) = await* insertSubDB(index);
                let insertId1 = await part3.startInserting({subDBKey = subDBKey3; sk = "name"; value = #text "xxx"});
                ignore await part3.finishInserting({dbOptions; index; insertId = insertId1});
                let insertId2 = await part3.startInserting({subDBKey = subDBKey3; sk = "name"; value = #text "xxx"}); // duplicate name
                ignore await part3.finishInserting({dbOptions; index; insertId = insertId2});
                let insertId3 = await part3.startInserting({subDBKey = subDBKey3; sk = "name2"; value = #text "yyy"});
                ignore await part3.finishInserting({dbOptions; index; insertId = insertId3});
                ActorSpec.assertAllTrue([
                    (await part3.subDBSize({subDBKey = subDBKey3})) == ?2,
                    (await part3.superDBSize()) == 3,
                ]);
            }),
            it("create a new partition canister", do {
                var counter = 0;
                shared func movingCallback({
                    oldCanister: Nac.PartitionCanister;
                    oldSubDBKey: Nac.SubDBKey;
                    newCanister: Nac.PartitionCanister;
                    newSubDBKey: Nac.SubDBKey;
                }) : async () {
                    counter += 1;
                };
                let index = await Index.Index(?(#numDBs 2));
                await index.init(?movingCallback);
                let insertId1 = await index.startCreatingSubDBDetailed({moveCap = #numDBs 2; movingCallback; hardCap = ?1000});
                ignore await index.finishCreatingSubDB({dbOptions = dbOptions2; index; creatingId = insertId1});
                let insertId2 = await index.startCreatingSubDBDetailed({moveCap = #numDBs 2; movingCallback; hardCap = ?1000});
                ignore await index.finishCreatingSubDB({dbOptions = dbOptions2; index; creatingId = insertId2});
                let insertId3 = await index.startCreatingSubDBDetailed({moveCap = #numDBs 2; movingCallback; hardCap = ?1000});
                let (part, subDBKey) = await index.finishCreatingSubDB({dbOptions = dbOptions2; index; creatingId = insertId3});

                ActorSpec.assertAllTrue([
                    counter == 0, // no item was moved
                    part == (await index.getCanisters())[1]
                ]);
            }),
            it("move overflowed DB", do {
                var counter = 0;
                shared func movingCallback({
                    oldCanister: Nac.PartitionCanister;
                    oldSubDBKey: Nac.SubDBKey;
                    newCanister: Nac.PartitionCanister;
                    newSubDBKey: Nac.SubDBKey;
                }) : async () {
                    counter += 1;
                };
                let index = await Index.Index(?(#usedMemory 50000));
                await index.init(?movingCallback);
                var address: ?(Nac.PartitionCanister, Nac.SubDBKey) = null;
                label cycle loop {
                    let creatingId = await index.startCreatingSubDBDetailed({moveCap = #numDBs 2; movingCallback; hardCap = ?1000});
                    address := ?(await index.finishCreatingSubDB({dbOptions = dbOptions2; index; creatingId}));
                    let ?(part, subDBKey) = address else {
                        Debug.trap("can't destructure address");
                    };
                    let canisters = await index.getCanisters();
                    if (Array.size(canisters) == 2 and part == canisters[1]) {
                        break cycle;
                    };
                };

                let ?(part, subDBKey) = address else {
                    Debug.trap("can't destructure address");
                };
                ActorSpec.assertAllTrue([
                    counter == 1,
                    part == (await index.getCanisters())[1],
                ]);
            }),
        ]),
    ]),
]);

if (success == false) {
  Debug.trap("Tests failed");
}
