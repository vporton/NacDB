import Debug "mo:base/Debug";
import ActorSpec "./utils/ActorSpec";
import Nac "../src/NacDB"

type Group = ActorSpec.Group;

let assertTrue = ActorSpec.assertTrue;
let describe = ActorSpec.describe;
let it = ActorSpec.it;
let skip = ActorSpec.skip;
let pending = ActorSpec.pending;
let run = ActorSpec.run;

let success = run([
    describe("Unit Test of NacDB", [
        describe("Subgroup", [
            it("should assess a boolean value", do {
                let index = Nac.createDBIndex();
                let superDB = Nac.createSuperDB({moveCap = #numDBs(1000)});
                assertTrue(true);
            }),
        ]),
    ]),
]);

if (success == false) {
  Debug.trap("Tests failed");
}
