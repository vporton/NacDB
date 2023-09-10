import Cycles "mo:base/ExperimentalCycles";
import Nac "../../../src/NacDB";
import Partition "../partition/main";
import StableBuffer "mo:stable-buffer/StableBuffer";
import Principal "mo:base/Principal";
import Debug "mo:base/Debug";
import MyCycles "../../../src/Cycles";

shared actor class Index(dbOptions: Nac.DBOptions) = this {
    stable var dbIndex: Nac.DBIndex = Nac.createDBIndex(dbOptions);

    stable var initialized = false;

    public shared func init() : async () {
        ignore MyCycles.topUpCycles(dbOptions.partitionCycles);
        if (initialized) {
            Debug.trap("already initialized");
        };
        MyCycles.addPart(dbOptions.partitionCycles);
        StableBuffer.add(dbIndex.canisters, await Partition.Partition(dbOptions));
        initialized := true;
    };

    public shared func createPartition(dbOptions: Nac.DBOptions): async Nac.PartitionCanister {
        ignore MyCycles.topUpCycles(dbOptions.partitionCycles);
        MyCycles.addPart(dbOptions.partitionCycles);
        await Partition.Partition(dbOptions);
    };

    public query func getCanisters(): async [Nac.PartitionCanister] {
        // ignore MyCycles.topUpCycles(dbOptions.partitionCycles);
        Nac.getCanisters(dbIndex);
    };

    public shared func newCanister(): async Nac.PartitionCanister {
        ignore MyCycles.topUpCycles(dbOptions.partitionCycles);
        await Nac.newCanister(this, dbIndex);
    };

    public shared func createSubDB({guid: Nac.GUID; userData: Text})
        : async {inner: (Nac.InnerCanister, Nac.InnerSubDBKey); outer: (Nac.OuterCanister, Nac.OuterSubDBKey)}
    {
        ignore MyCycles.topUpCycles(dbOptions.partitionCycles);
        await* Nac.createSubDB({guid; index = this; dbIndex; dbOptions; userData});
    };
}