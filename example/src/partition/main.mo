import I "mo:base/Iter";
import RBT "mo:stable-rbtree/StableRBTree";
import Nac "../../../src/NacDB";
import Principal "mo:base/Principal";
import Bool "mo:base/Bool";
import Nat "mo:base/Nat";
import SparseQueue "../../../lib/SparseQueue";
import Text "mo:base/Text";

shared({caller}) actor class Partition(dbOptions: Nac.DBOptions) = this {
    stable let index: Nac.IndexCanister = actor(Principal.toText(caller));

    // let dbOptions = dbOptions;

    stable let superDB = Nac.createSuperDB();

    // Mandatory methods //

    public shared func rawInsertSubDB(map: RBT.Tree<Nac.SK, Nac.AttributeValue>, userData: Text, dbOptions: Nac.DBOptions) : async Nac.SubDBKey {
        Nac.rawInsertSubDB(superDB, map, userData, dbOptions);
    };

    public shared func isOverflowed({dbOptions: Nac.DBOptions}) : async Bool {
        Nac.isOverflowed({dbOptions; superDB});
    };

    public shared func createSubDB({dbOptions: Nac.DBOptions; userData: Text}) : async Nat {
        Nac.rawInsertSubDB(superDB, RBT.init(), userData, dbOptions);
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

    public shared func startInserting({subDBKey: Nac.SubDBKey; sk: Nac.SK; value: Nac.AttributeValue}) : async Nat {
        await* Nac.startInserting({
            dbOptions;
            indexCanister = index;
            currentCanister = this;
            superDB = superDB;
            subDBKey;
            sk;
            value;
        })
    };

    public shared func finishInserting({index: Nac.IndexCanister; dbOptions: Nac.DBOptions; insertId: SparseQueue.SparseQueueKey}): async (Nac.PartitionCanister, Nac.SubDBKey) {
        await* Nac.finishInserting({index; oldSuperDB = superDB; dbOptions; insertId});
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