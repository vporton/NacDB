import Cycles "mo:base/ExperimentalCycles";
import Nac "../../../src/NacDB";
import Partition "../partition/main";
import StableBuffer "mo:stable-buffer/StableBuffer";
import Principal "mo:base/Principal";
import Debug "mo:base/Debug";
import Blob "mo:base/Blob";
import Array "mo:base/Array";
import Iter "mo:base/Iter";
import Result "mo:base/Result";
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
        Principal.fromActor(await Partition.Partition());
    };

    public query func getCanisters(): async [Principal] {
        // ignore MyCycles.topUpCycles(Common.dbOptions.partitionCycles);
        let iter = Iter.map(Nac.getCanisters(dbIndex).vals(), func (x: Nac.PartitionCanister): Principal {Principal.fromActor(x)});
        Iter.toArray(iter);
    };

    public shared func createPartitionImpl(): async Principal {
        ignore MyCycles.topUpCycles(Common.dbOptions.partitionCycles);
        await* Nac.createPartitionImpl(this, dbIndex);
    };

    public shared func createSubDB({guid: [Nat8]; userData: Text})
        : async {inner: (Principal, Nac.InnerSubDBKey); outer: (Principal, Nac.OuterSubDBKey)}
    {
        ignore MyCycles.topUpCycles(Common.dbOptions.partitionCycles);
        let r = await* Nac.createSubDB({guid = Blob.fromArray(guid); index = this; dbIndex; dbOptions = Common.dbOptions; userData});
        { inner = (Principal.fromActor(r.inner.0), r.inner.1); outer = (Principal.fromActor(r.outer.0), r.outer.1) };
    };

    public shared func finishMovingSubDBImpl({
        guid: [Nat8];
        index: Principal;
        outerCanister: Principal;
        oldInnerCanister: Principal;
        outerKey: Nac.OuterSubDBKey;
        oldInnerKey: Nac.InnerSubDBKey;
    }) : async (Principal, Nac.InnerSubDBKey) {
        ignore MyCycles.topUpCycles(Common.dbOptions.partitionCycles);
        let index2: Nac.IndexCanister = actor(Principal.toText(index));
        let outerCanister2: Nac.OuterCanister = actor(Principal.toText(outerCanister));
        let oldInnerCanister2: Nac.OuterCanister = actor(Principal.toText(oldInnerCanister));
        let (inner, key) = await* Nac.finishMovingSubDBImpl({
            dbIndex;
            guid = Blob.fromArray(guid);
            index = index2;
            outerCanister = outerCanister2;
            outerKey;
            oldInnerCanister = oldInnerCanister2;
            oldInnerKey;
        });
        (Principal.fromActor(inner), key);
    };

    public shared func insert({
        guid: [Nat8];
        outerCanister: Principal;
        outerKey: Nac.OuterSubDBKey;
        sk: Nac.SK;
        value: Nac.AttributeValue;
    }) : async Result.Result<{inner: (Principal, Nac.InnerSubDBKey); outer: (Principal, Nac.OuterSubDBKey)}, Text> {
        ignore MyCycles.topUpCycles(Common.dbOptions.partitionCycles);
        let res = await* Nac.insert({
            guid = Blob.fromArray(guid);
            indexCanister = Principal.fromActor(this);
            dbIndex;
            outerCanister = outerCanister;
            outerKey;
            sk;
            value;
        });
        switch (res) {
            case (#ok { inner; outer }) {
                #ok { inner = (Principal.fromActor(inner.0), inner.1); outer = (Principal.fromActor(outer.0), outer.1) };
            };
            case (#err err) { #err err };
        };
    };

    public shared func delete({outerCanister: Principal; outerKey: Nac.OuterSubDBKey; sk: Nac.SK; guid: [Nat8]}): async () {
        let outer: Partition.Partition = actor(Principal.toText(outerCanister));
        ignore MyCycles.topUpCycles(Common.dbOptions.partitionCycles);
        await* Nac.delete({dbIndex; outerCanister = outer; outerKey; sk; guid = Blob.fromArray(guid)});
    };
}