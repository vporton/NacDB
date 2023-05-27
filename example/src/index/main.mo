import Nac "../../../src/NacDB";
import Partition "../partition/main";
import StableBuffer "mo:stable-buffer/StableBuffer";

shared actor class Index() = this {
    var index: Nac.DBIndex = Nac.createDBIndex();

    public shared func getCanisters(): async [Principal] {
        StableBuffer.toArray(index.canisters);
    };

    public shared func newCanister(): async Partition.Partition {
        let canister = await Partition.Partition();
    };
}