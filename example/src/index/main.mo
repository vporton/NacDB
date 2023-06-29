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

    public shared func startInsertingSubDB() : async Nat {
        await* Nac.startCreatingSubDB({dbIndex = index; dbOptions = {
            hardCap = ?hardCap;
            movingCallback = movingCallback;
            maxSubDBsInCreating = 15;
        }});
    };

    public shared func finishInsertingSubDB(creatingId: Nat) : async (Nac.PartitionCanister, Nac.SubDBKey) {
        // TODO: React on state update code here.
        let (part, subDBKey) = await* Nac.finishCreatingSubDB({
            creatingId;
            index = this;
            dbIndex = index;
            dbOptions = {
                hardCap = ?hardCap;
                movingCallback = movingCallback;
                maxSubDBsInCreating = 15;
            }
        });
        (part, subDBKey);
    };

    // Intended for testing only.
    public shared func insertSubDBDetailed({hardCap: ?Nat}) : async (Nac.PartitionCanister, Nac.SubDBKey) {
        let (part, subDBKey) = await* Nac.startCreatingSubDB({dbIndex = index; dbOptions = {
            hardCap;
            movingCallback = movingCallback;
            maxSubDBsInCreating = 15;
        }});
        // TODO: React on state update code here.
        await* Nac.finishCreatingSubDB(index);
        (part, subDBKey);
    };

    public shared func creatingSubDBKeys() : async [Nac.SubDBKey] {
        Nac.creatingSubDBKeys(index);
    };

    public shared func finishInsertSubDB(subDBKey: Nac.SubDBKey) : async () {
        await* Nac.finishCreatingSubDB(index)
    };
}