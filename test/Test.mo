import Debug "mo:base/Debug";
import ActorSpec "./utils/ActorSpec";
import Nac "../src/NacDB";
import Index "../example/src/index/main";
import Partition "../example/src/partition/main";
import Principal "mo:base/Principal";

type Group = ActorSpec.Group;

let assertTrue = ActorSpec.assertTrue;
let describe = ActorSpec.describe;
let it = ActorSpec.it;
let skip = ActorSpec.skip;
let pending = ActorSpec.pending;
let run = ActorSpec.run;

func createCanisters() : async* {index: Index.Index} {
    let index = await Index.Index();
    await index.init(null); // TODO: `movingCallback`
    {index};
};

func insertSubDB(index: Index.Index) : async* (Partition.Partition, Nac.SubDBKey) {
    let (part, subDBKey) = await index.insertSubDB();
    (
        actor(Principal.toText(Principal.fromActor(part))),
        subDBKey,
    );
};

func createSubDB() : async* {index: Index.Index; part: Partition.Partition; subDBKey: Nac.SubDBKey}
{
    let {index} = await* createCanisters();
    let (part, subDBKey) = await* insertSubDB(index);
    {
        index = actor(Principal.toText(Principal.fromActor(index)));
        part;
        subDBKey;
    }
};

let success = run([
    describe("Unit Test of NacDB", [
        describe("Simple DB operations", [
            it("insert/get", do {
                let {index; part; subDBKey} = await* createSubDB();
                let name = "Dummy";
                await part.insert({subDBKey = subDBKey; sk = "name"; value = #text name});
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
                await part.insert({subDBKey = subDBKey; sk = "name"; value = #text name});
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
                await part3.insert({subDBKey = subDBKey3; sk = "name"; value = #text "xxx"});
                await part3.insert({subDBKey = subDBKey3; sk = "name"; value = #text "xxx"}); // duplicate name
                await part3.insert({subDBKey = subDBKey3; sk = "name2"; value = #text "yyy"});
                ActorSpec.assertAllTrue([
                    (await part3.subDBSize({subDBKey = subDBKey3})) == ?2,
                    (await part3.superDBSize()) == 3,
                ]);
            }),
            it("move to a new partition canister", do {
                var counter = 0;
                shared func movingCallback({
                    oldCanister: Nac.PartitionCanister;
                    oldSubDBKey: Nac.SubDBKey;
                    newCanister: Nac.PartitionCanister;
                    newSubDBKey: Nac.SubDBKey;
                }) : async () {
                    counter += 1;
                };
                let index = await Index.Index();
                await index.init(?movingCallback);
                let (partx1, subDBKey1) = await index.insertSubDBDetailed({hardCap = ?2});
                let (partx2, subDBKey2) = await index.insertSubDBDetailed({hardCap = ?2});
                let (partx3, subDBKey3) = await index.insertSubDBDetailed({hardCap = ?2});

                ActorSpec.assertAllTrue([counter == 1]);
            }),
        ]),
    ]),
]);

if (success == false) {
  Debug.trap("Tests failed");
}
