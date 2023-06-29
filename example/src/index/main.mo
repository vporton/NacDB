import Cycles "mo:base/ExperimentalCycles";
import Nac "../../../src/NacDB";
import Partition "../partition/main";
import StableBuffer "mo:stable-buffer/StableBuffer";
import Principal "mo:base/Principal";
import Debug "mo:base/Debug";

shared actor class Index() = this {
    // TODO: Not good to duplicate in two places:
    let moveCap = #usedMemory 500_000;
    let dbOptions = {moveCap; movingCallback = null; hardCap = ?1000; maxSubDBsInCreating = 15};

    stable var index: Nac.DBIndex = Nac.createDBIndex({moveCap});

    stable var movingCallbackV: ?Nac.MovingCallback = null; // TODO: Rename.

    public shared func movingCallback({
        newCanister : Nac.PartitionCanister;
        newSubDBKey : Nac.SubDBKey;
        oldCanister : Nac.PartitionCanister;
        oldSubDBKey : Nac.SubDBKey
    }): async () {
        ignore do ? { await movingCallbackV!({newCanister; newSubDBKey; oldCanister; oldSubDBKey}); };
    };

    public shared func init(movingCallbackValue: ?Nac.MovingCallback) : async () {
        movingCallbackV := movingCallbackValue;
        Cycles.add(300_000_000_000); // TODO: duplicate line of code
        // TODO: `StableBuffer` is too low level.
        StableBuffer.add(index.canisters, await Partition.Partition());
    };

    public query func getCanisters(): async [Nac.PartitionCanister] {
        StableBuffer.toArray(index.canisters);
    };

    public shared func newCanister(): async Nac.PartitionCanister {
        Cycles.add(300_000_000_000); // TODO: duplicate line of code
        let canister = await Partition.Partition();
    };

    public shared func startInsertingSubDB() : async Nat {
        await* Nac.startCreatingSubDB({dbIndex = index; dbOptions});
    };

    public shared func finishInsertingSubDB(creatingId: Nat) : async (Nac.PartitionCanister, Nac.SubDBKey) {
        // TODO: React on state update code here.
        let (part, subDBKey) = await* Nac.finishCreatingSubDB({
            creatingId;
            index = this;
            dbIndex = index;
            dbOptions;
        });
        (part, subDBKey);
    };

    // Intended for testing only.
    public shared func startInsertingSubDBDetailed({hardCap: ?Nat}) : async Nat {
        let creatingId = await* Nac.startCreatingSubDB({dbIndex = index; dbOptions = {
            dbOptions;
            hardCap;
            moveCap;
            movingCallback = ?movingCallback;
            maxSubDBsInCreating = 15;
        }});
    };
}