import I "mo:base/Iter";
import RBT "mo:stable-rbtree/StableRBTree";
import Nac "../../../src/NacDB";
import Principal "mo:base/Principal";
import Bool "mo:base/Bool";
import Nat "mo:base/Nat";
import SparseQueue "../../../lib/SparseQueue";
import MyCycles "../../../lib/Cycles";
import Text "mo:base/Text";

shared({caller}) actor class Partition(dbOptions: Nac.DBOptions) = this {
    stable let index: Nac.IndexCanister = actor(Principal.toText(caller));

    // let dbOptions = dbOptions;

    stable let superDB = Nac.createSuperDB();

    // Mandatory methods //

    public shared func rawInsertSubDB(map: RBT.Tree<Nac.SK, Nac.AttributeValue>, userData: Text, dbOptions: Nac.DBOptions)
        : async {inner: Nac.InnerSubDBKey; outer: Nac.OuterSubDBKey; wasOld: Bool}
    {
        ignore MyCycles.topUpCycles(150_000_000_000);
        Nac.rawInsertSubDB(this, superDB, map, userData, dbOptions);
    };

    public shared func isOverflowed({dbOptions: Nac.DBOptions}) : async Bool {
        ignore MyCycles.topUpCycles(150_000_000_000);
        Nac.isOverflowed({dbOptions; superDB});
    };

    // Some data access methods //

    public query func superDBSize() : async Nat {
        ignore MyCycles.topUpCycles(150_000_000_000);
        Nac.superDBSize(superDB);
    };

    public shared func deleteSubDB({outerKey: Nac.OuterSubDBKey}) : async () {
        ignore MyCycles.topUpCycles(150_000_000_000);
        await* Nac.deleteSubDB({dbOptions; outerSuperDB = superDB; outerKey});
    };

    public shared func deleteSubDBInner(innerKey: Nac.InnerSubDBKey) : async () {
        ignore MyCycles.topUpCycles(150_000_000_000);
        await* Nac.deleteSubDBInner(superDB, innerKey);
    };

    public shared func finishMovingSubDBImpl({
        guid: Nac.GUID;
        index: Nac.IndexCanister;
        outerCanister: Nac.PartitionCanister;
        outerKey: Nac.OuterSubDBKey;
        oldInnerKey: Nac.InnerSubDBKey;
        dbOptions: Nac.DBOptions;
    }) : async (Nac.PartitionCanister, Nac.InnerSubDBKey) {
        ignore MyCycles.topUpCycles(150_000_000_000);
        await* Nac.finishMovingSubDBImpl({
            oldInnerSuperDB = superDB;
            guid;
            index;
            outerCanister;
            outerKey;
            oldInnerKey;
            dbOptions;
        })
    };

    public shared func insert({
        guid: Nac.GUID;
        dbOptions: Nac.DBOptions;
        indexCanister: Nac.IndexCanister;
        outerCanister: Nac.PartitionCanister;
        outerKey: Nac.OuterSubDBKey;
        sk: Nac.SK;
        value: Nac.AttributeValue;
    }) : async {inner: (Nac.PartitionCanister, Nac.InnerSubDBKey); outer: (Nac.PartitionCanister, Nac.OuterSubDBKey)} {
        ignore MyCycles.topUpCycles(150_000_000_000);
        await* Nac.insert({
            guid;
            dbOptions;
            indexCanister;
            outerCanister;
            outerSuperDB = superDB;
            outerKey;
            sk;
            value;
        });
    };

    public shared func putLocation(outerKey: Nac.OuterSubDBKey, innerCanister: Nac.PartitionCanister, newInnerSubDBKey: Nac.InnerSubDBKey) : async () {
        ignore MyCycles.topUpCycles(150_000_000_000);
        Nac.putLocation(superDB, outerKey, innerCanister, newInnerSubDBKey);
    };

    public shared func createOuter(part: Nac.PartitionCanister, innerKey: Nac.InnerSubDBKey)
        : async {inner: (Nac.PartitionCanister, Nac.InnerSubDBKey); outer: (Nac.PartitionCanister, Nac.OuterSubDBKey)}
    {
        ignore MyCycles.topUpCycles(150_000_000_000);
        Nac.createOuter(superDB, part, innerKey);
    };

    public shared func delete({outerKey: Nac.OuterSubDBKey; sk: Nac.SK}): async () {
        ignore MyCycles.topUpCycles(150_000_000_000);
        await* Nac.delete({dbOptions; outerSuperDB = superDB; outerKey; sk});
    };

    public shared func deleteInner(innerKey: Nac.InnerSubDBKey, sk: Nac.SK): async () {
        ignore MyCycles.topUpCycles(150_000_000_000);
        await* Nac.deleteInner({innerSuperDB = superDB; innerKey; sk});
    };

    public query func scanLimitInner({innerKey: Nac.InnerSubDBKey; lowerBound: Nac.SK; upperBound: Nac.SK; dir: RBT.Direction; limit: Nat})
        : async RBT.ScanLimitResult<Text, Nac.AttributeValue>
    {
        ignore MyCycles.topUpCycles(150_000_000_000);
        Nac.scanLimitInner({innerSuperDB = superDB; innerKey; lowerBound; upperBound; dir; limit});
    };

    public shared func scanLimitOuter({outerKey: Nac.OuterSubDBKey; lowerBound: Text; upperBound: Text; dir: RBT.Direction; limit: Nat})
        : async RBT.ScanLimitResult<Text, Nac.AttributeValue>
    {
        ignore MyCycles.topUpCycles(150_000_000_000);
        await* Nac.scanLimitOuter({dbOptions; outerSuperDB = superDB; outerKey; lowerBound; upperBound; dir; limit});
    };

    public query func getByInner({subDBKey: Nac.InnerSubDBKey; sk: Nac.SK}): async ?Nac.AttributeValue {
        ignore MyCycles.topUpCycles(150_000_000_000);
        Nac.getByInner({superDB; subDBKey; sk});
    };

    public query func hasByInner({subDBKey: Nac.InnerSubDBKey; sk: Nac.SK}): async Bool {
        ignore MyCycles.topUpCycles(150_000_000_000);
        Nac.hasByInner({superDB; subDBKey; sk});
    };

    public shared func getByOuter({subDBKey: Nac.OuterSubDBKey; sk: Nac.SK}): async ?Nac.AttributeValue {
        ignore MyCycles.topUpCycles(150_000_000_000);
        await* Nac.getByOuter({dbOptions; outerSuperDB = superDB; outerKey = subDBKey; sk});
    };

    public shared func hasByOuter({subDBKey: Nac.OuterSubDBKey; sk: Nac.SK}): async Bool {
        ignore MyCycles.topUpCycles(150_000_000_000);
        await* Nac.hasByOuter({dbOptions; outerSuperDB = superDB; outerKey = subDBKey; sk});
    };

    public shared func hasSubDBByOuter(options: {subDBKey: Nac.OuterSubDBKey}): async Bool {
        ignore MyCycles.topUpCycles(150_000_000_000);
        await* Nac.hasSubDBByOuter({outerSuperDB = superDB; outerKey = options.subDBKey});
    };

    public query func hasSubDBByInner(options: {subDBKey: Nac.InnerSubDBKey}): async Bool {
        ignore MyCycles.topUpCycles(150_000_000_000);
        Nac.hasSubDBByInner({innerSuperDB = superDB; innerKey = options.subDBKey});
    };

    public shared func subDBSizeByOuter({subDBKey: Nac.OuterSubDBKey}): async ?Nat {
        ignore MyCycles.topUpCycles(150_000_000_000);
        await* Nac.subDBSizeByOuter({dbOptions; outerSuperDB = superDB; outerKey = subDBKey});
    };

    public query func subDBSizeByInner({subDBKey: Nac.InnerSubDBKey}): async ?Nat {
        ignore MyCycles.topUpCycles(150_000_000_000);
        Nac.subDBSizeByInner({superDB; subDBKey});
    };

    public shared func startInsertingImpl({
        guid: Nac.GUID;
        dbOptions: Nac.DBOptions;
        indexCanister: Nac.IndexCanister;
        outerCanister: Nac.PartitionCanister;
        outerKey: Nac.OuterSubDBKey;
        sk: Nac.SK;
        value: Nac.AttributeValue;
        innerKey: Nac.InnerSubDBKey;
    }): async () {
        ignore MyCycles.topUpCycles(150_000_000_000);
        await* Nac.startInsertingImpl({
            guid;
            dbOptions;
            indexCanister;
            outerCanister;
            outerKey;
            sk;
            value;
            innerSuperDB = superDB;
            innerKey;
        });
    };
}