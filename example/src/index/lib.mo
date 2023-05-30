import Nac "../../../src/NacDB";
import Partition "../partition";
import StableBuffer "mo:stable-buffer/StableBuffer";
import Principal "mo:base/Principal";
import Debug "mo:base/Debug";

shared actor class Index() = this {
    let hardCap = 1000;

    stable var index: Nac.DBIndex = Nac.createDBIndex();

    public shared func init() : async () {
        // TODO: too low level
        StableBuffer.add(index.canisters, Principal.fromActor(await Partition.Partition()));
    };

    public query func getCanisters(): async [Principal] {
        StableBuffer.toArray(index.canisters);
    };

    public shared func newCanister(): async Partition.Partition {
        let canister = await Partition.Partition();
    };

    public shared func insertSubDB() : async (Nac.PartitionCanister, Nac.SubDBKey) {
        // TODO: For this kind of operation no need for two stages?
        let (part, subDBKey) = await* Nac.creatingSubDBStage1({dbIndex = index; hardCap = ?1000});
        await* Nac.creatingSubDBStage2(index, subDBKey);
        (part, subDBKey);
    };
}