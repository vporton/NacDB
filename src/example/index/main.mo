import Nac "../../../src/NacDB";
import Partition "../partition/main";
import StableBuffer "mo:stable-buffer/StableBuffer";
import Principal "mo:base/Principal";
import Debug "mo:base/Debug";
import Blob "mo:base/Blob";
import Iter "mo:base/Iter";
import Result "mo:base/Result";
import MyCycles "../../../src/Cycles";
import Common "../common";

shared actor class Index() = this {
    stable var dbIndex: Nac.DBIndex = Nac.createDBIndex(Common.dbOptions);

    stable var initialized = false;

    public shared func init() : async () {
        ignore MyCycles.topUpCycles<system>(Common.dbOptions.partitionCycles);
        if (initialized) {
            Debug.trap("already initialized");
        };
        MyCycles.addPart<system>(Common.dbOptions.partitionCycles);
        StableBuffer.add(dbIndex.canisters, await Partition.Partition());
        initialized := true;
    };

    public shared func createPartition(): async Principal {
        ignore MyCycles.topUpCycles<system>(Common.dbOptions.partitionCycles);
        MyCycles.addPart<system>(Common.dbOptions.partitionCycles);
        Principal.fromActor(await Partition.Partition());
    };

    public query func getCanisters(): async [Principal] {
        // ignore MyCycles.topUpCycles<system>(Common.dbOptions.partitionCycles);
        let iter = Iter.map(Nac.getCanisters(dbIndex).vals(), func (x: Nac.PartitionCanister): Principal {Principal.fromActor(x)});
        Iter.toArray(iter);
    };

    public shared func createSubDB(guid: [Nat8], {userData: Text; hardCap: ?Nat})
        : async {inner: {canister: Principal; key: Nac.InnerSubDBKey}; outer: {canister: Principal; key: Nac.OuterSubDBKey}}
    {
        ignore MyCycles.topUpCycles<system>(Common.dbOptions.partitionCycles);
        let r = await* Nac.createSubDB(Blob.fromArray(guid), {index = this; dbIndex; dbOptions = Common.dbOptions; userData; hardCap});
        {
            inner = {canister = Principal.fromActor(r.inner.canister); key = r.inner.key};
            outer = {canister = Principal.fromActor(r.outer.canister); key = r.outer.key};
        };
    };

    public shared func insert(guid: [Nat8], {
        outerCanister: Principal;
        outerKey: Nac.OuterSubDBKey;
        sk: Nac.SK;
        value: Nac.AttributeValue;
        hardCap: ?Nat;
    }) : async Result.Result<{inner: {canister: Principal; key: Nac.InnerSubDBKey}; outer: {canister: Principal; key: Nac.OuterSubDBKey}}, Text> {
        ignore MyCycles.topUpCycles<system>(Common.dbOptions.partitionCycles);
        let res = await* Nac.insert(Blob.fromArray(guid), {
            indexCanister = Principal.fromActor(this);
            dbIndex;
            outerCanister = outerCanister;
            outerKey;
            sk;
            value;
            hardCap;
        });
        switch (res) {
            case (#ok { inner; outer }) {
                #ok {
                    inner = { canister = Principal.fromActor(inner.canister); key = inner.key};
                    outer = { canister = Principal.fromActor(outer.canister); key = outer.key};
                };
            };
            case (#err err) { #err err };
        };
    };

    public shared func delete(guid: [Nat8], {outerCanister: Principal; outerKey: Nac.OuterSubDBKey; sk: Nac.SK}): async () {
        let outer: Partition.Partition = actor(Principal.toText(outerCanister));
        ignore MyCycles.topUpCycles<system>(Common.dbOptions.partitionCycles);
        await* Nac.delete(Blob.fromArray(guid), {dbIndex; outerCanister = outer; outerKey; sk});
    };

    public shared func deleteSubDB(guid: [Nat8], {outerCanister: Principal; outerKey: Nac.OuterSubDBKey}) : async () {
        ignore MyCycles.topUpCycles<system>(Common.dbOptions.partitionCycles);
        await* Nac.deleteSubDB(Blob.fromArray(guid), {dbIndex; dbOptions = Common.dbOptions; outerCanister = actor(Principal.toText(outerCanister)); outerKey});
    };
}