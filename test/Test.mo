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

func createSubDB() : async {index: Index.Index; part: Nac.PartitionCanister; subDBKey: Nac.SubDBKey}
{
    let {index} = await createCanisters();
    let (part, subDBKey) = await index.insertSubDB();
    {index; part; subDBKey}
};

let success = run([
    describe("Unit Test of NacDB", [
        describe("Simple DB operations", [
            it("insert/get", do {
                let {index; part; subDBKey} = await createSubDB();
                let name = "Dummy";
                await part.insert({subDBKey = subDBKey; sk = "name"; value = #text name});
                let name2 = await part.get({subDBKey; sk = "name"});
                ActorSpec.assertTrue(name2 == ?(#text name));
            }),
        ]),
    ]),
]);

if (success == false) {
  Debug.trap("Tests failed");
}
