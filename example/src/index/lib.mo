import Nac "../../../src/NacDB";
import Partition "../partition";
import StableBuffer "mo:stable-buffer/StableBuffer";
import Principal "mo:base/Principal";

shared actor class Index() = this {
    let hardCap = 1000;

    stable var index: Nac.DBIndex = Nac.createDBIndex();

    public shared func init() : async () {
        // TODO: too low level
        StableBuffer.add(index.canisters, Principal.fromActor(await Partition.Partition()));
    };

    public shared func getCanisters(): async [Principal] {
        StableBuffer.toArray(index.canisters);
    };

    public shared func newCanister(): async Partition.Partition {
        let canister = await Partition.Partition();
    };
}