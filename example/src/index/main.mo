import Cycles "mo:base/ExperimentalCycles";
import Nac "../../../src/NacDB";
import Partition "../partition/main";
import StableBuffer "mo:stable-buffer/StableBuffer";
import Principal "mo:base/Principal";
import Debug "mo:base/Debug";
import Blob "mo:base/Blob";
import MyCycles "../../../src/Cycles";
import Common "../common";

shared actor class Index() = this {
    stable var dbIndex: Nac.DBIndex = Nac.createDBIndex(Common.dbOptions);

    stable var initialized = false;

    public shared func init() : async () {
        ignore MyCycles.topUpCycles(Common.dbOptions.partitionCycles);
        if (initialized) {
            Debug.trap("already initialized");
        };
        MyCycles.addPart(Common.dbOptions.partitionCycles);
        StableBuffer.add(dbIndex.canisters, await Partition.Partition());
        initialized := true;
    };

    public shared func createPartition(): async Principal {
        ignore MyCycles.topUpCycles(Common.dbOptions.partitionCycles);
        MyCycles.addPart(Common.dbOptions.partitionCycles);
        await Partition.Partition();
    };

    public query func getCanisters(): async [Principal] {
        // ignore MyCycles.topUpCycles(Common.dbOptions.partitionCycles);
        Nac.getCanisters(dbIndex);
    };

    public shared func createPartitionImpl(): async Principal {
        ignore MyCycles.topUpCycles(Common.dbOptions.partitionCycles);
        await Nac.createPartitionImpl(this, dbIndex);
    };

    public shared func createSubDB({guid: [Nat8]; userData: Text})
        : async {inner: (Principal, Nac.InnerSubDBKey); outer: (Principal, Nac.OuterSubDBKey)}
    {
        ignore MyCycles.topUpCycles(Common.dbOptions.partitionCycles);
        let r = await* Nac.createSubDB({guid = Blob.fromArray(guid); index = this; dbIndex; dbOptions = Common.dbOptions; userData});
        { inner = (Principal.fromActor(r.inner.0), r.inner.1); outer = (Principal.fromActor(r.outer.0), r.outer.1) };
    };
}