import Nat64 "mo:base/Nat64";
import Array "mo:base/Array";
import Buffer "mo:base/Buffer";
import Sha256 "mo:sha2/Sha256";
import Blob "mo:base/Blob";
import Iter "mo:base/Iter";
import Nat8 "mo:base/Nat8";

module {
    public type GUID = Blob;

    public type GUIDGenerator = {
        seed: [Nat8];
        var step: Nat;
    };

    public func init(seed: [Nat8]): GUIDGenerator {
        {seed; var step = 0};
    };

    func myEncodeNat(n: Nat): [Nat8] {
        var n64 = Nat64.fromNat(n);
        let buf = Buffer.Buffer<Nat8>(8);
        for (i in Iter.range(0, 7)) {
            buf.add(Nat8.fromNat(Nat64.toNat(n64 % 256)));
            n64 >>= 8;
        };
        Buffer.toArray(buf);
    };

    public func nextGuid(gen: GUIDGenerator): GUID {
        let step = Nat64.fromNat(gen.step);
        gen.step += 1;
        var buf = Buffer.Buffer<Nat8>(Array.size(gen.seed) + 8);
        buf.append(Buffer.fromArray(gen.seed));
        buf.append(Buffer.fromArray(myEncodeNat(Nat64.toNat(step))));
        let hash = Blob.toArray(Sha256.fromArray(#sha256, Buffer.toArray(buf)));
        let shortHash = Array.subArray(hash, 0, 16);
        Blob.fromArray(shortHash);
    };
}