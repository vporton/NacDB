import I "mo:base/Iter";
import RBT "mo:stable-rbtree/StableRBTree";
import Nac "../../../src/NacDB";
import Principal "mo:base/Principal";
import Bool "mo:base/Bool";
import Nat "mo:base/Nat";

shared({caller}) actor class Partition() = this {
    stable let index: Nac.IndexCanister = actor(Principal.toText(caller));

    stable let superDB = Nac.createSuperDB({moveCap = #usedMemory 500_000; moveCallback = null; createCallback = null});

    // Mandatory methods //

    public shared func rawInsertSubDB(data: RBT.Tree<Nac.SK, Nac.AttributeValue>, dbOptions: Nac.DBOptions) : async Nac.SubDBKey {
        Nac.rawInsertSubDB(superDB, data, dbOptions);
    };

    public shared func isOverflowed() : async Bool {
        Nac.isOverflowed(superDB);
    };

    public shared func createSubDB({dbOptions: Nac.DBOptions}) : async Nat {
        Nac.createSubDB({superDB; dbOptions});
    };

    public shared func releaseSubDB(subDBKey: Nac.SubDBKey) : async () {
        await* Nac.releaseSubDB(superDB, subDBKey);
    };

    // Some data access methods //

    public query func get(options: {subDBKey: Nac.SubDBKey; sk: Nac.SK}) : async ?Nac.AttributeValue {
        Nac.get({superDB; subDBKey = options.subDBKey; sk = options.sk});
    };

    public query func has(options: {subDBKey: Nac.SubDBKey; sk: Nac.SK}) : async Bool {
        Nac.has({superDB; subDBKey = options.subDBKey; sk = options.sk});
    };

    public query func hasSubDB(options: {subDBKey: Nac.SubDBKey}) : async Bool {
        Nac.hasSubDB({superDB; subDBKey = options.subDBKey});
    };

    public query func superDBSize() : async Nat {
        Nac.superDBSize(superDB);
    };

    public query func subDBSize({subDBKey: Nac.SubDBKey}) : async ?Nat {
        Nac.subDBSize({superDB; subDBKey});
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
    };

    public shared func delete({subDBKey: Nac.SubDBKey; sk: Nac.SK}) : async () {
        Nac.delete({superDB; subDBKey; sk});
    };

    public shared func deleteSubDB({subDBKey: Nac.SubDBKey}) : async () {
        Nac.deleteSubDB({superDB; subDBKey});
    };

    public shared func scanLimit({subDBKey: Nac.SubDBKey; lowerBound: Text; upperBound: Text; dir: RBT.Direction; limit: Nat}) : async Nac.ScanLimitResult {
        Nac.scanLimit({superDB; subDBKey; lowerBound; upperBound; dir; limit});
    };
}