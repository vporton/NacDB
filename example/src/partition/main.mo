import RBT "mo:stable-rbtree/StableRBTree";
import Nac "../../../src/NacDB";
import Principal "mo:base/Principal";

shared({caller}) actor class Partition() = this {
    stable let index: Nac.IndexCanister = actor(Principal.toText(caller)); // FIXME: wrong caller

    stable let superDB = Nac.createSuperDB({moveCap = #usedMemory 500_000; moveCallback = null; createCallback = null});

    stable var subDBKey: ?Nac.SubDBKey = null; // FIXME: not here

    // Mandatory methods //

    let movingCallback: shared ({
        oldCanister: Nac.PartitionCanister;
        oldSubDBKey: Nac.SubDBKey;
        newCanister: Nac.PartitionCanister;
        newSubDBKey: Nac.SubDBKey;
    }) -> async () = index.movingCallback;

    public shared func rawInsertSubDB(data: RBT.Tree<Nac.SK, Nac.AttributeValue>, dbOptions: Nac.DBOptions) : async Nac.SubDBKey {
        Nac.rawInsertSubDB(superDB, data, dbOptions);
    };

    public shared func isOverflowed() : async Bool {
        Nac.isOverflowed(superDB);
    };

    // public shared func getSubDB() : async ?Nac.SubDB {
    //     Nac.getSubDB(superDB);
    // };

    public shared func createSubDB({dbOptions: Nac.DBOptions; busy: Bool}) : async Nat {
        Nac.createSubDB({superDB; dbOptions; busy});
    };

    public shared func releaseSubDB(subDBKey: Nac.SubDBKey) : async () {
        await Nac.releaseSubDB(superDB, subDBKey);
    };

    // Some data access methods //

    public query func get(options: {subDBKey: Nac.SubDBKey; sk: Nac.SK}) : async ?Nac.AttributeValue {
        Nac.get({superDB; subDBKey = options.subDBKey; sk = options.sk})
    };

    public shared func insert({subDBKey: Nac.SubDBKey; sk: Nac.SK; value: Nac.AttributeValue}) : async () {
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