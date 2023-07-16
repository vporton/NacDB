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

    public shared func rawInsertSubDB(map: RBT.Tree<Nac.SK, Nac.AttributeValue>, userData: Text, dbOptions: Nac.DBOptions)
        : async {inner: Nac.InnerSubDBKey; outer: Nac.OuterSubDBKey}
    {
        Nac.rawInsertSubDB(this, superDB, map, userData, dbOptions);
    };

    public shared func isOverflowed({dbOptions: Nac.DBOptions}) : async Bool {
        Nac.isOverflowed({dbOptions; superDB});
    };

    // Some data access methods //

    public query func superDBSize() : async Nat {
        Nac.superDBSize(superDB);
    };

    public shared func deleteSubDB({outerKey: Nac.OuterSubDBKey}) : async () {
        Nac.deleteSubDB({superDB; outerKey});
    };

    public shared func deleteSubDBInner(innerKey: Nac.InnerSubDBKey) : async () {
        Nac.deleteSubDBInner(superDB, innerKey);
    };


    public shared func finishMovingSubDBImpl({
        guid: Nac.GUID;
        index: Nac.IndexCanister;
        outerCanister: Nac.PartitionCanister;
        outerKey: Nac.OuterSubDBKey;
        oldInnerKey: Nac.InnerSubDBKey;
        dbOptions: Nac.DBOptions;
    }) : async (Nac.PartitionCanister, Nac.InnerSubDBKey) {
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
        Nac.putLocation(superDB, outerKey, innerCanister, newInnerSubDBKey);
    };

    public shared func createOuter(part: Nac.PartitionCanister, innerKey: Nac.InnerSubDBKey)
        : async {inner: (Nac.PartitionCanister, Nac.InnerSubDBKey); outer: (Nac.PartitionCanister, Nac.OuterSubDBKey)}
    {
        createOuter(superDB, part, innerKey);
    };

    public shared func delete({outerKey: Nac.OuterSubDBKey; sk: Nac.SK}): async () {
        Nac.delete({outerSuperDB = superDB; outerKey; sk});
    };

    public shared func deleteInner(innerKey: Nac.InnerSubDBKey, sk: Nac.SK): async () {
        Nac.deleteInner({innerSuperDB = superDB; innerKey; sk});
    };

    public query func scanLimitInner({innerKey: Nac.InnerSubDBKey; lowerBound: Nac.SK; upperBound: Nac.SK; dir: RBT.Direction; limit: Nat})
        : async RBT.ScanLimitResult<Text, Nac.AttributeValue>
    {
        Nac.scanLimitInner({innerSuperDB = superDB; innerKey; lowerBound; upperBound; dir; limit});
    };

    public query func scanLimitOuter({outerSuperDB: Nac.SuperDB; outerKey: Nac.OuterSubDBKey; lowerBound: Text; upperBound: Text; dir: RBT.Direction; limit: Nat})
        : async RBT.ScanLimitResult<Text, Nac.AttributeValue>
    {
        Nac.scanLimitOuter({outerSuperDB = superDB; outerKey; lowerBound; upperBound; dir; limit});
    };

    public query func getByInner(options: {subDBKey: Nac.InnerSubDBKey; sk: Nac.SK}): async ?Nac.AttributeValue {
        Nac.getByInner({superDB; subDBKey; sk});
    };

    public query func hasByInner(options: {subDBKey: Nac.InnerSubDBKey; sk: Nac.SK}): async Bool {
        Nac.hasByInner({superDB; subDBKey; sk});
    };

    public query func getByOuter(options: {subDBKey: Nac.OuterSubDBKey; sk: Nac.SK}): async ?Nac.AttributeValue {
        Nac.getByOuter({superDB; subDBKey; sk});
    };

    public query func hasByOuter(options: {subDBKey: Nac.OuterSubDBKey; sk: Nac.SK}): async Bool {
        Nac.hasByOuter({superDB; subDBKey; sk});
    };

    public query func hasSubDBByInner(options: {subDBKey: Nac.InnerSubDBKey}): async Bool {
        hasSubDBByInner({innerSuperDB = superDB; innerKey: subDBKey});
    };

    public query func subDBSizeByInner(options: {subDBKey: Nac.InnerSubDBKey}): async Bool {
        subDBSizeByInner({innerSuperDB = superDB; innerKey: subDBKey});
    };

    public shared func startInsertingImpl(options: {
        guid: Nac.GUID;
        dbOptions: Nac.DBOptions;
        indexCanister: Nac.IndexCanister;
        outerCanister: Nac.PartitionCanister;
        outerKey: Nac.OuterSubDBKey;
        sk: Nac.SK;
        value: Nac.AttributeValue;
        innerSuperDB: Nac.SuperDB;
        innerKey: Nac.InnerSubDBKey;
    }): async () {
        startInsertingImpl({
            guid;
            dbOptions;
            indexCanister;
            outerCanister;
            outerSuperDB = superDB;
            outerKey;
            sk;
            value;
            innerSuperDB;
            innerKey;
        });
    };
}