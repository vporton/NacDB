import RBT "mo:stable-rbtree/StableRBTree";
import Nac "../../../src/NacDB";
import Principal "mo:base/Principal";

shared({caller}) actor class Partition() = this {
    stable let index: Nac.IndexCanister = actor(Principal.toText(caller)); // FIXME: wrong caller

    stable let superDB = Nac.createSuperDB({moveCap = #usedMemory 500_000; moveCallback = null; createCallback = null});

    stable var subDBKey: ?Nac.SubDBKey = null;

    // Mandatory methods //

    public shared func insertSubDB() {
        subDBKey := ?Nac.creatingSubDBStage1({canister = this; superDB = superDB; hardCap = ?1000});
        // Here process changes of subDBKey.
        Nac.creatingSubDBStage2(superDB);
    };

    // TODO: `hardCap` not here.
    public shared func rawInsertSubDB(data: RBT.Tree<Nac.SK, Nac.AttributeValue>, hardCap: ?Nat) : async Nac.SubDBKey {
        Nac.rawInsertSubDB(superDB, data, hardCap);
    };

    public shared func isOverflowed() : async Bool {
        Nac.isOverflowed(superDB);
    };

    // Some data access methods //

    public shared func get(options: {subDBKey: Nac.SubDBKey; sk: Nac.SK}) : async ?Nac.AttributeValue {
        Nac.get({superDB; subDBKey = options.subDBKey; sk = options.sk})
    };

    public shared func insert({subDBKey: Nac.SubDBKey; sk: Nac.SK; value: Nac.AttributeValue}) {
        await* Nac.insert({
            indexCanister = index;
            currentCanister = this;
            superDB = superDB;
            subDBKey;
            sk;
            value;
        })
    }
}