import Cycles "mo:base/ExperimentalCycles";
import Nac "../../../src/NacDB";
import Partition "../partition/main";
import StableBuffer "mo:stable-buffer/StableBuffer";
import Principal "mo:base/Principal";
import Debug "mo:base/Debug";

shared actor class Index() = this {
    let hardCap = 1000;

    stable var index: Nac.DBIndex = Nac.createDBIndex();

    stable var movingCallback: ?Nac.MovingCallback = null;

    public shared func init(movingCallbackValue: ?Nac.MovingCallback) : async () {
        movingCallback := movingCallbackValue;
        Cycles.add(300_000_000_000); // TODO: duplicate line of code
        // TODO: `StableBuffer` is too low level.
        StableBuffer.add(index.canisters, Principal.fromActor(await Partition.Partition()));
    };

    public query func getCanisters(): async [Principal] {
        StableBuffer.toArray(index.canisters);
    };

    public shared func newCanister(): async Partition.Partition {
        Cycles.add(300_000_000_000); // TODO: duplicate line of code
        let canister = await Partition.Partition();
    };

    public shared func insertSubDB() : async (Nac.PartitionCanister, Nac.SubDBKey) {
        // TODO: For this kind of operation no need for two stages?
        let (part, subDBKey) = await* Nac.creatingSubDBStage1({dbIndex = index; dbOptions = {
            hardCap = ?1000;
            movingCallback = movingCallback;
            maxSubDBsInCreating = 15;
        }});
        // TODO: React on state update code here.
        await* Nac.creatingSubDBStage2(index, subDBKey);
        (part, subDBKey);
    };
}