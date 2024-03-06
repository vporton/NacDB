import I "mo:base/Iter";
import BTree "mo:stableheapbtreemap/BTree";
import Nac "../../../src/NacDB";
import Principal "mo:base/Principal";
import Bool "mo:base/Bool";
import Nat "mo:base/Nat";
import MyCycles "../../../src/Cycles";
import Text "mo:base/Text";
import Debug "mo:base/Debug";
import Blob "mo:base/Blob";
import Iter "mo:base/Iter";
import Common "../common";

shared({caller}) actor class Partition() = this {
    stable let index: Nac.IndexCanister = actor(Principal.toText(caller));

    stable let superDB = Nac.createSuperDB(Common.dbOptions);

    // Mandatory methods //

    public query func rawGetSubDB({innerKey: Nac.InnerSubDBKey}): async ?{map: [(Nac.SK, Nac.AttributeValue)]; userData: Text} {
        // ignore MyCycles.topUpCycles(Common.dbOptions.partitionCycles);
        Nac.rawGetSubDB(superDB, innerKey);
    };

    public shared func rawInsertSubDB({map: [(Nac.SK, Nac.AttributeValue)]; innerKey: ?Nac.InnerSubDBKey; userData: Text; hardCap: ?Nat})
        : async {innerKey: Nac.OuterSubDBKey}
    {
        ignore MyCycles.topUpCycles(Common.dbOptions.partitionCycles);
        Nac.rawInsertSubDB({superDB; map; innerKey; userData; hardCap});
    };

    public shared func rawDeleteSubDB({innerKey: Nac.InnerSubDBKey}): async () {
        ignore MyCycles.topUpCycles(Common.dbOptions.partitionCycles);
        Nac.rawDeleteSubDB(superDB, innerKey);
    };

    public shared func rawInsertSubDBAndSetOuter({
        map: [(Nac.SK, Nac.AttributeValue)];
        keys: ?{
            innerKey: Nac.InnerSubDBKey;
            outerKey: Nac.OuterSubDBKey;
        };
        userData: Text;
        hardCap: ?Nat;
    })
        : async {innerKey: Nac.InnerSubDBKey; outerKey: Nac.OuterSubDBKey}
    {
        ignore MyCycles.topUpCycles(Common.dbOptions.partitionCycles);
        Nac.rawInsertSubDBAndSetOuter({superDB; canister = this; map; keys; userData; hardCap});
    };

    public query func isOverflowed() : async Bool {
        // ignore MyCycles.topUpCycles(Common.dbOptions.partitionCycles);
        Nac.isOverflowed({dbOptions = Common.dbOptions; superDB});
    };

    // Some data access methods //

    public query func getInner({outerKey: Nac.OuterSubDBKey}) : async ?(Principal, Nac.InnerSubDBKey) {
        // ignore MyCycles.topUpCycles(Common.dbOptions.partitionCycles);
        switch (Nac.getInner({superDB; outerKey})) {
            case (?(innerCanister, innerKey)) {
                ?(Principal.fromActor(innerCanister), innerKey);
            };
            case null { null };
        };
    };

    public query func superDBSize() : async Nat {
        // ignore MyCycles.topUpCycles(Common.dbOptions.partitionCycles);
        Nac.superDBSize(superDB);
    };

    public shared func deleteSubDBInner({innerKey: Nac.InnerSubDBKey}) : async () {
        ignore MyCycles.topUpCycles(Common.dbOptions.partitionCycles);
        await* Nac.deleteSubDBInner({superDB; innerKey});
    };

    public shared func putLocation({outerKey: Nac.OuterSubDBKey; innerCanister: Principal; newInnerSubDBKey: Nac.InnerSubDBKey}) : async () {
        ignore MyCycles.topUpCycles(Common.dbOptions.partitionCycles);
        let inner2: Nac.InnerCanister = actor(Principal.toText(innerCanister));
        Nac.putLocation({outerSuperDB = superDB; outerKey; innerCanister = inner2; innerKey = newInnerSubDBKey});
    };

    public shared func createOuter({part: Principal; outerKey: Nac.OuterSubDBKey; innerKey: Nac.InnerSubDBKey})
        : async {inner: (Principal, Nac.InnerSubDBKey); outer: (Principal, Nac.OuterSubDBKey)}
    {
        ignore MyCycles.topUpCycles(Common.dbOptions.partitionCycles);
        let part2: Nac.PartitionCanister = actor(Principal.toText(part));
        let { inner; outer } = Nac.createOuter({outerSuperDB = superDB; part = part2; outerKey; innerKey});
        { inner = (Principal.fromActor(inner.0), inner.1); outer = (Principal.fromActor(outer.0), outer.1) };
    };

    public shared func deleteInner({innerKey: Nac.InnerSubDBKey; sk: Nac.SK}): async () {
        ignore MyCycles.topUpCycles(Common.dbOptions.partitionCycles);
        await* Nac.deleteInner({innerSuperDB = superDB; innerKey; sk});
    };

    public query func scanLimitInner({innerKey: Nac.InnerSubDBKey; lowerBound: Nac.SK; upperBound: Nac.SK; dir: BTree.Direction; limit: Nat})
        : async BTree.ScanLimitResult<Text, Nac.AttributeValue>
    {
        // ignore MyCycles.topUpCycles(Common.dbOptions.partitionCycles);
        Nac.scanLimitInner({innerSuperDB = superDB; innerKey; lowerBound; upperBound; dir; limit});
    };

    public shared func scanLimitOuter({outerKey: Nac.OuterSubDBKey; lowerBound: Text; upperBound: Text; dir: BTree.Direction; limit: Nat})
        : async BTree.ScanLimitResult<Text, Nac.AttributeValue>
    {
        ignore MyCycles.topUpCycles(Common.dbOptions.partitionCycles);
        await* Nac.scanLimitOuter({outerSuperDB = superDB; outerKey; lowerBound; upperBound; dir; limit});
    };

    public query func scanSubDBs(): async [(Nac.OuterSubDBKey, (Principal, Nac.InnerSubDBKey))] {
        // ignore MyCycles.topUpCycles(Common.dbOptions.partitionCycles);
        type T1 = (Nac.OuterSubDBKey, (Nac.InnerCanister, Nac.InnerSubDBKey));
        type T2 = (Nac.OuterSubDBKey, (Principal, Nac.InnerSubDBKey));
        let array: [T1] = Nac.scanSubDBs({superDB});
        let iter = Iter.map(array.vals(), func ((outerKey, (inner, innerKey)): T1): T2 {
            (outerKey, (Principal.fromActor(inner), innerKey));
        });
        Iter.toArray(iter);
    };

    public query func getByInner({innerKey: Nac.InnerSubDBKey; sk: Nac.SK}): async ?Nac.AttributeValue {
        // ignore MyCycles.topUpCycles(Common.dbOptions.partitionCycles);
        Nac.getByInner({superDB; innerKey; sk});
    };

    public query func hasByInner({innerKey: Nac.InnerSubDBKey; sk: Nac.SK}): async Bool {
        // ignore MyCycles.topUpCycles(Common.dbOptions.partitionCycles);
        Nac.hasByInner({superDB; innerKey; sk});
    };

    public shared func getByOuter({outerKey: Nac.OuterSubDBKey; sk: Nac.SK}): async ?Nac.AttributeValue {
        ignore MyCycles.topUpCycles(Common.dbOptions.partitionCycles);
        await* Nac.getByOuter({outerSuperDB = superDB; outerKey; sk});
    };

    public shared func hasByOuter({outerKey: Nac.OuterSubDBKey; sk: Nac.SK}): async Bool {
        ignore MyCycles.topUpCycles(Common.dbOptions.partitionCycles);
        await* Nac.hasByOuter({outerSuperDB = superDB; outerKey; sk});
    };

    public shared func hasSubDBByOuter(options: {outerKey: Nac.OuterSubDBKey}): async Bool {
        ignore MyCycles.topUpCycles(Common.dbOptions.partitionCycles);
        await* Nac.hasSubDBByOuter({outerSuperDB = superDB; outerKey = options.outerKey});
    };

    public query func hasSubDBByInner(options: {innerKey: Nac.InnerSubDBKey}): async Bool {
        // ignore MyCycles.topUpCycles(Common.dbOptions.partitionCycles);
        Nac.hasSubDBByInner({innerSuperDB = superDB; innerKey = options.innerKey});
    };

    public shared func subDBSizeByOuter({outerKey: Nac.OuterSubDBKey}): async ?Nat {
        ignore MyCycles.topUpCycles(Common.dbOptions.partitionCycles);
        await* Nac.subDBSizeByOuter({outerSuperDB = superDB; outerKey});
    };

    public query func subDBSizeByInner({innerKey: Nac.InnerSubDBKey}): async ?Nat {
        // ignore MyCycles.topUpCycles(Common.dbOptions.partitionCycles);
        Nac.subDBSizeByInner({superDB; innerKey});
    };

    public shared func startInsertingImpl({
        guid: [Nat8];
        indexCanister: Principal;
        outerCanister: Principal;
        outerKey: Nac.OuterSubDBKey;
        sk: Nac.SK;
        value: Nac.AttributeValue;
        innerKey: Nac.InnerSubDBKey;
        needsMove: Bool;
    }): async () {
        ignore MyCycles.topUpCycles(Common.dbOptions.partitionCycles);
        let index: Nac.IndexCanister = actor(Principal.toText(indexCanister));
        let outer: Nac.OuterCanister = actor(Principal.toText(outerCanister));
        await* Nac.startInsertingImpl({
            guid = Blob.fromArray(guid);
            indexCanister = index;
            outerCanister = outer;
            outerKey;
            sk;
            value;
            innerSuperDB = superDB;
            innerKey;
            needsMove;
        });
    };

    public shared func getSubDBUserDataInner(options: {innerKey: Nac.InnerSubDBKey}) : async ?Text {
        Nac.getSubDBUserDataInner({superDB; subDBKey = options.innerKey});
    };

    public shared func deleteSubDBOuter({outerKey: Nac.OuterSubDBKey}) : async () {
        await* Nac.deleteSubDBOuter({superDB; outerKey});
    };

    public shared func getOuter(options: Nac.GetByOuterPartitionKeyOptions) : async ?Nac.AttributeValue {
        await* Nac.getOuter(options, Common.dbOptions);
    };

    public shared func hasByOuterPartitionKey(options: Nac.HasByOuterPartitionKeyOptions) : async Bool {
        await* Nac.hasByOuterPartitionKey(options);
    };

    public shared func getSubDBUserDataOuter(options: Nac.GetUserDataOuterOptions) : async ?Text {
        await* Nac.getSubDBUserDataOuter(options, Common.dbOptions);
    };

    public shared func subDBSizeOuter(options: Nac.SubDBSizeOuterOptions): async ?Nat {
        await* Nac.subDBSizeOuter(options, Common.dbOptions);
    };
}