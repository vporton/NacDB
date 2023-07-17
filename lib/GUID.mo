import Binary "mo:encoding/Binary";
import Nat64 "mo:base/Nat64";
import Array "mo:base/Array";
import Buffer "mo:base/Buffer";
import Sha "mo:sha/SHA256";
import Blob "mo:base/Blob";

module {
    public type GUID = Blob;

    public type GUIDGenerator = {
        seed: [Nat8];
        var step: Nat;
    };

    public func init(seed: [Nat8]): GUIDGenerator {
        {seed; var step = 0};
    };

    public func nextGuid(gen: GUIDGenerator): GUID {
        let step = Nat64.fromNat(gen.step);
        gen.step += 1;
        var buf = Buffer.Buffer<Nat8>(Array.size(gen.seed) + 8);
        buf.append(Buffer.fromArray(gen.seed));
        buf.append(Buffer.fromArray(Binary.LittleEndian.fromNat64(step)));
        let hash = Sha.sha256(Buffer.toArray(buf));
        let shortHash = Array.subArray(hash, 0, 16);
        Blob.fromArray(shortHash);
    };
}