import RBT "mo:stable-rbtree/StableRBTree";
import Nac "../../../src/NacDB";

shared({caller}) actor class Partition() = this {
    let index = caller;

    let superDB = Nac.createSuperDB({moveCap = #usedMemory 500_000; moveCallback = null});

    // Mandatory methods //

    public shared func insertSubDB(data: RBT.Tree<Nac.SK, Nac.AttributeValue>) : async Nac.SubDBKey {
        // FIXME: `data` not here.
        Nac.insertSubDB(superDB, {var data = data; hardCap = ?2000});
    };

    public shared func isOverflowed() : async Bool {
        Nac.isOverflowed(superDB);
    };

    // Some data access methods //

    public shared func get(options: {subDBKey: Nac.SubDBKey; sk: Nac.SK}) : async ?Nac.AttributeValue {
        Nac.get({superDB; subDBKey = options.subDBKey; sk = options.sk})
    };

//     public shared func insertOrCreate(options: {subDBKey: SubDBKey; sk: SK}) : async ?Nac.AttributeValue {
//         Nac.get({superDB; subDBKey; sk})
//     };
}