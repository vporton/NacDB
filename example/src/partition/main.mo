import RBT "mo:stable-rbtree/StableRBTree";
import Nac "../../../src/NacDB";

shared({caller}) actor class Partition() = this {
    stable let index = caller;

    stable let superDB = Nac.createSuperDB({moveCap = #usedMemory 500_000; moveCallback = null; createCallback = null});

    // Mandatory methods //

    public shared func insertSubDB(data: RBT.Tree<Nac.SK, Nac.AttributeValue>) : async Nac.SubDBKey {
        // FIXME: `data` not here.
        // FIXME: `busy` not here.
        Nac.insertSubDB(superDB, {var data = data; hardCap = ?2000; var busy = false});
    };

    public shared func isOverflowed() : async Bool {
        Nac.isOverflowed(superDB);
    };

    // Some data access methods //

    public shared func get(options: {subDBKey: Nac.SubDBKey; sk: Nac.SK}) : async ?Nac.AttributeValue {
        Nac.get({superDB; subDBKey = options.subDBKey; sk = options.sk})
    };

}