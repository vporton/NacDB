import Cycles "mo:base/ExperimentalCycles";
import Nac "../../../src/NacDB";
import Partition "../partition/main";
import StableBuffer "mo:stable-buffer/StableBuffer";
import Principal "mo:base/Principal";
import Debug "mo:base/Debug";

shared actor class Index(dbOptions: Nac.DBOptions) = this {
    stable var dbIndex: Nac.DBIndex = Nac.createDBIndex(dbOptions);

    public shared func movingCallback({
        newCanister : Nac.PartitionCanister;
        newSubDBKey : Nac.SubDBKey;
        oldCanister : Nac.PartitionCanister;
        oldSubDBKey : Nac.SubDBKey
    }): async () {
        ignore do ? { await dbOptions.movingCallback!({newCanister; newSubDBKey; oldCanister; oldSubDBKey}); };
    };

    public shared func init() : async () {
        Cycles.add(300_000_000_000); // TODO: duplicate line of code
        // TODO: `StableBuffer` is too low level.
        StableBuffer.add(dbIndex.canisters, await Partition.Partition(dbOptions));
    };

    public query func getCanisters(): async [Nac.PartitionCanister] {
        StableBuffer.toArray(dbIndex.canisters);
    };

    public shared func newCanister(): async Nac.PartitionCanister {
        Cycles.add(300_000_000_000); // TODO: duplicate line of code
        let canister = await Partition.Partition(dbOptions);
        StableBuffer.add(dbIndex.canisters, canister); // TODO: too low level
        canister;
    };

    public shared func startCreatingSubDB({dbOptions : Nac.DBOptions}) : async Nat {
        await* Nac.startCreatingSubDB({dbIndex; dbOptions});
    };

    public shared func finishCreatingSubDB({creatingId : Nat; dbOptions : Nac.DBOptions; index : Nac.IndexCanister})
        : async (Nac.PartitionCanister, Nac.SubDBKey)
    {
        // TODO: React on state update code here.
        let (part, subDBKey) = await* Nac.finishCreatingSubDB({
            creatingId;
            index = this;
            dbIndex;
            dbOptions;
        });
        (part, subDBKey);
    };
}