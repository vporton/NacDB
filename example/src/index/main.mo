import Cycles "mo:base/ExperimentalCycles";
import Nac "../../../src/NacDB";
import Partition "../partition/main";
import StableBuffer "mo:stable-buffer/StableBuffer";
import Principal "mo:base/Principal";
import Debug "mo:base/Debug";

shared actor class Index(dbOptions: Nac.DBOptions) = this {
    stable var dbIndex: Nac.DBIndex = Nac.createDBIndex(dbOptions);

    stable var initialized = false;

    public shared func init() : async () {
        if (initialized) {
            Debug.trap("already initialized");
        };
        Cycles.add(300_000_000_000); // TODO: duplicate line of code
        // TODO: `StableBuffer` is too low level.
        StableBuffer.add(dbIndex.canisters, await Partition.Partition(dbOptions));
        initialized := true;
    };

    public query func getCanisters(): async [Nac.PartitionCanister] {
        Nac.getCanisters(dbIndex);
    };

    public shared func newCanister(): async Nac.PartitionCanister {
        await* Nac.newCanister(dbOptions, dbIndex);
    };

    public shared func createSubDB({guid: Nac.GUID; dbOptions: Nac.DBOptions; userData: Text})
        : async {inner: (Nac.PartitionCanister, Nac.InnerSubDBKey); outer: (Nac.PartitionCanister, Nac.OuterSubDBKey)}
    {
        await* Nac.createSubDB({guid; dbIndex; dbOptions; userData});
    };
}