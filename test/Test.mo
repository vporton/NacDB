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

func createCanisters() : async {index: Index.Index} {
    let index = await Index.Index();
    await index.init(null); // TODO: `movingCallback`
    {index};
};

func createSubDB() : async {index: Index.Index; part: Partition.Partition; subDBKey: Nac.SubDBKey}
{
    let {index} = await createCanisters();
    let (part, subDBKey) = await index.insertSubDB();
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
                let {index; part; subDBKey} = await createSubDB();
                let name = "Dummy";
                await part.insert({subDBKey = subDBKey; sk = "name"; value = #text name});
                let name2 = await part.get({subDBKey; sk = "name"});
                let has = await part.has({subDBKey; sk = "name"});
                ActorSpec.assertAllTrue([
                    name2 == ?(#text name),
                    has,
                ]);
            }),
            it("insert/get miss", do {
                let {index; part; subDBKey} = await createSubDB();
                let name = "Dummy";
                await part.insert({subDBKey = subDBKey; sk = "name"; value = #text name});
                let name2 = await part.get({subDBKey; sk = "namex"});
                let has = await part.has({subDBKey; sk = "namex"});
                ActorSpec.assertAllTrue([
                    name2 == null,
                    not has,
                ]);
            }),
        ]),
    ]),
]);

if (success == false) {
  Debug.trap("Tests failed");
}
