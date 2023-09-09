import I "mo:base/Iter";
import BTree "mo:btree/BTree";
import Nac "../../../src/NacDB";
import Principal "mo:base/Principal";
import Bool "mo:base/Bool";
import Nat "mo:base/Nat";
import MyCycles "../../../lib/Cycles";
import Text "mo:base/Text";
import Debug "mo:base/Debug";

shared({caller}) actor class Partition(dbOptions: Nac.DBOptions) = this {
    stable let index: Nac.IndexCanister = actor(Principal.toText(caller));

    stable let superDB = Nac.createSuperDB(dbOptions);

    // Mandatory methods //

    public shared func rawInsertSubDB(map: [(Nac.SK, Nac.AttributeValue)], inner: ?Nac.InnerSubDBKey, userData: Text)
        : async {inner: Nac.OuterSubDBKey}
    {
        ignore MyCycles.topUpCycles(dbOptions.partitionCycles);
        Nac.rawInsertSubDB(superDB, map, inner, userData);
    };

    public shared func rawInsertSubDBAndSetOuter(
        map: [(Nac.SK, Nac.AttributeValue)],
        keys: ?{
            inner: Nac.InnerSubDBKey;
            outer: Nac.OuterSubDBKey;
        },
        userData: Text,
    )
        : async {inner: Nac.InnerSubDBKey; outer: Nac.OuterSubDBKey}
    {
        ignore MyCycles.topUpCycles(dbOptions.partitionCycles);
        Nac.rawInsertSubDBAndSetOuter(superDB, this, map, keys, userData);
    };

    public shared func isOverflowed({}) : async Bool {
        ignore MyCycles.topUpCycles(dbOptions.partitionCycles);
        Nac.isOverflowed({dbOptions; superDB});
    };

    // Some data access methods //

    public query func superDBSize() : async Nat {
        // ignore MyCycles.topUpCycles(dbOptions.partitionCycles);
        Nac.superDBSize(superDB);
    };

    public shared func deleteSubDB({outerKey: Nac.OuterSubDBKey; guid: Nac.GUID}) : async () {
        ignore MyCycles.topUpCycles(dbOptions.partitionCycles);
        await* Nac.deleteSubDB({dbOptions; outerSuperDB = superDB; outerKey; guid});
    };

    public shared func deleteSubDBInner(innerKey: Nac.InnerSubDBKey) : async () {
        ignore MyCycles.topUpCycles(dbOptions.partitionCycles);
        await* Nac.deleteSubDBInner(superDB, innerKey);
    };

    public shared func finishMovingSubDBImpl({
        guid: Nac.GUID;
        index: Nac.IndexCanister;
        outerCanister: Nac.OuterCanister;
        outerKey: Nac.OuterSubDBKey;
        oldInnerKey: Nac.InnerSubDBKey;
    }) : async (Nac.InnerCanister, Nac.InnerSubDBKey) {
        ignore MyCycles.topUpCycles(dbOptions.partitionCycles);
        await* Nac.finishMovingSubDBImpl({
            oldInnerSuperDB = superDB;
            guid;
            index;
            outerCanister;
            outerKey;
            oldInnerKey;
        })
    };

    public shared func insert({
        guid: Nac.GUID;
        indexCanister: Nac.IndexCanister;
        outerCanister: Nac.OuterCanister;
        outerKey: Nac.OuterSubDBKey;
        sk: Nac.SK;
        value: Nac.AttributeValue;
    }) : async {inner: (Nac.InnerCanister, Nac.InnerSubDBKey); outer: (Nac.OuterCanister, Nac.OuterSubDBKey)} {
        ignore MyCycles.topUpCycles(dbOptions.partitionCycles);
        await* Nac.insert({
            guid;
            indexCanister;
            outerCanister;
            outerSuperDB = superDB;
            outerKey;
            sk;
            value;
        });
    };

    public shared func putLocation(outerKey: Nac.OuterSubDBKey, innerCanister: Nac.InnerCanister, newInnerSubDBKey: Nac.InnerSubDBKey) : async () {
        ignore MyCycles.topUpCycles(dbOptions.partitionCycles);
        Nac.putLocation(superDB, outerKey, innerCanister, newInnerSubDBKey);
    };

    public shared func createOuter(part: Nac.PartitionCanister, outerKey: Nac.OuterSubDBKey, innerKey: Nac.InnerSubDBKey)
        : async {inner: (Nac.InnerCanister, Nac.InnerSubDBKey); outer: (Nac.OuterCanister, Nac.OuterSubDBKey)}
    {
        ignore MyCycles.topUpCycles(dbOptions.partitionCycles);
        Nac.createOuter(superDB, part, outerKey, innerKey);
    };

    public shared func delete({outerKey: Nac.OuterSubDBKey; sk: Nac.SK; guid: Nac.GUID}): async () {
        ignore MyCycles.topUpCycles(dbOptions.partitionCycles);
        await* Nac.delete({outerSuperDB = superDB; outerKey; sk; guid});
    };

    public shared func deleteInner(innerKey: Nac.InnerSubDBKey, sk: Nac.SK): async () {
        ignore MyCycles.topUpCycles(dbOptions.partitionCycles);
        await* Nac.deleteInner({innerSuperDB = superDB; innerKey; sk});
    };

    public query func scanLimitInner({innerKey: Nac.InnerSubDBKey; lowerBound: Nac.SK; upperBound: Nac.SK; dir: BTree.Direction; limit: Nat})
        : async BTree.ScanLimitResult<Text, Nac.AttributeValue>
    {
        // ignore MyCycles.topUpCycles(dbOptions.partitionCycles);
        Nac.scanLimitInner({innerSuperDB = superDB; innerKey; lowerBound; upperBound; dir; limit});
    };

    public shared func scanLimitOuter({outerKey: Nac.OuterSubDBKey; lowerBound: Text; upperBound: Text; dir: BTree.Direction; limit: Nat})
        : async BTree.ScanLimitResult<Text, Nac.AttributeValue>
    {
        ignore MyCycles.topUpCycles(dbOptions.partitionCycles);
        await* Nac.scanLimitOuter({outerSuperDB = superDB; outerKey; lowerBound; upperBound; dir; limit});
    };

    public query func scanSubDBs(): async [(Nac.OuterSubDBKey, (Nac.InnerCanister, Nac.InnerSubDBKey))] {
        // ignore MyCycles.topUpCycles(dbOptions.partitionCycles);
        Nac.scanSubDBs({superDB});
    };

    public query func getByInner({subDBKey: Nac.InnerSubDBKey; sk: Nac.SK}): async ?Nac.AttributeValue {
        // ignore MyCycles.topUpCycles(dbOptions.partitionCycles);
        Nac.getByInner({superDB; subDBKey; sk});
    };

    public query func hasByInner({subDBKey: Nac.InnerSubDBKey; sk: Nac.SK}): async Bool {
        // ignore MyCycles.topUpCycles(dbOptions.partitionCycles);
        Nac.hasByInner({superDB; subDBKey; sk});
    };

    public shared func getByOuter({subDBKey: Nac.OuterSubDBKey; sk: Nac.SK}): async ?Nac.AttributeValue {
        ignore MyCycles.topUpCycles(dbOptions.partitionCycles);
        await* Nac.getByOuter({outerSuperDB = superDB; outerKey = subDBKey; sk});
    };

    public shared func hasByOuter({subDBKey: Nac.OuterSubDBKey; sk: Nac.SK}): async Bool {
        ignore MyCycles.topUpCycles(dbOptions.partitionCycles);
        await* Nac.hasByOuter({outerSuperDB = superDB; outerKey = subDBKey; sk});
    };

    public shared func hasSubDBByOuter(options: {subDBKey: Nac.OuterSubDBKey}): async Bool {
        ignore MyCycles.topUpCycles(dbOptions.partitionCycles);
        await* Nac.hasSubDBByOuter({outerSuperDB = superDB; outerKey = options.subDBKey});
    };

    public query func hasSubDBByInner(options: {subDBKey: Nac.InnerSubDBKey}): async Bool {
        // ignore MyCycles.topUpCycles(dbOptions.partitionCycles);
        Nac.hasSubDBByInner({innerSuperDB = superDB; innerKey = options.subDBKey});
    };

    public shared func subDBSizeByOuter({subDBKey: Nac.OuterSubDBKey}): async ?Nat {
        ignore MyCycles.topUpCycles(dbOptions.partitionCycles);
        await* Nac.subDBSizeByOuter({outerSuperDB = superDB; outerKey = subDBKey});
    };

    public query func subDBSizeByInner({subDBKey: Nac.InnerSubDBKey}): async ?Nat {
        // ignore MyCycles.topUpCycles(dbOptions.partitionCycles);
        Nac.subDBSizeByInner({superDB; subDBKey});
    };

    public shared func startInsertingImpl({
        guid: Nac.GUID;
        indexCanister: Nac.IndexCanister;
        outerCanister: Nac.OuterCanister;
        outerKey: Nac.OuterSubDBKey;
        sk: Nac.SK;
        value: Nac.AttributeValue;
        innerKey: Nac.InnerSubDBKey;
        needsMove: Bool;
    }): async () {
        ignore MyCycles.topUpCycles(dbOptions.partitionCycles);
        await* Nac.startInsertingImpl({
            guid;
            indexCanister;
            outerCanister;
            outerKey;
            sk;
            value;
            innerSuperDB = superDB;
            innerKey;
            needsMove;
        });
    };

    public func getSubDBUserDataOuter(options: {subDBKey: Nac.OuterSubDBKey}) : async ?Text {
        await* Nac.getSubDBUserDataOuter({superDB; subDBKey = options.subDBKey});
    };

    public func getSubDBUserDataInner(options: {subDBKey: Nac.OuterSubDBKey}) : async ?Text {
        Nac.getSubDBUserDataInner({superDB; subDBKey = options.subDBKey});
    };
}