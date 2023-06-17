import Debug "mo:base/Debug";

import ActorSpec "./utils/ActorSpec";
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
                assertTrue(true);
            }),
        ]),
    ]),
]);

if(success == false){
  Debug.trap("Tests failed");
}
