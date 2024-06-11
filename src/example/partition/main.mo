import BTree "mo:stableheapbtreemap/BTree";
import Nac "../../../src/NacDB";
import Principal "mo:base/Principal";
import Bool "mo:base/Bool";
import Nat "mo:base/Nat";
import MyCycles "mo:cycles-simple";
import Text "mo:base/Text";
import Iter "mo:base/Iter";
import Common "../common";

shared({caller}) actor class Partition() = this {
    stable let superDB = Nac.createSuperDB(Common.dbOptions);

    // Mandatory methods //

    public query func rawGetSubDB({innerKey: Nac.InnerSubDBKey}): async ?{map: [(Nac.SK, Nac.AttributeValue)]; userData: Text} {
        // Nac.rawGetSubDB(superDB, innerKey);
    };

    public shared func rawInsertSubDB({map: [(Nac.SK, Nac.AttributeValue)]; innerKey: ?Nac.InnerSubDBKey; userData: Text; hardCap: ?Nat})
        : async {innerKey: Nac.InnerSubDBKey}
    {
        Nac.rawInsertSubDB({superDB; map; innerKey; userData; hardCap});
    };

    public shared func rawDeleteSubDB({innerKey: Nac.InnerSubDBKey}): async () {
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
        Nac.rawInsertSubDBAndSetOuter({superDB; canister = this; map; keys; userData; hardCap});
    };

    public query func isOverflowed() : async Bool {
        // Nac.isOverflowed({dbOptions = Common.dbOptions; superDB});
    };

    // Some data access methods //

    public query func getInner({outerKey: Nac.OuterSubDBKey}) : async ?{canister: Principal; key: Nac.InnerSubDBKey} {
        switch (Nac.getInner({superDB; outerKey})) {
            case (?{canister = innerCanister; key = innerKey}) {
                ?{canister = Principal.fromActor(innerCanister); key = innerKey};
            };
            case null { null };
        };
    };

    public query func superDBSize() : async Nat {
        // Nac.superDBSize(superDB);
    };

    public shared func deleteSubDBInner({innerKey: Nac.InnerSubDBKey}) : async () {
        await* Nac.deleteSubDBInner({superDB; innerKey});
    };

    public shared func putLocation({outerKey: Nac.OuterSubDBKey; innerCanister: Principal; innerKey: Nac.InnerSubDBKey}) : async () {
        let inner2: Nac.InnerCanister = actor(Principal.toText(innerCanister));
        Nac.putLocation({outerSuperDB = superDB; outerKey; innerCanister = inner2; innerKey});
    };

    public shared func createOuter({part: Principal; outerKey: Nac.OuterSubDBKey; innerKey: Nac.InnerSubDBKey})
        : async {inner: {canister: Principal; key: Nac.InnerSubDBKey}; outer: {canister: Principal; key: Nac.OuterSubDBKey}}
    {
        let part2: Nac.PartitionCanister = actor(Principal.toText(part));
        let { inner; outer } = Nac.createOuter({outerSuperDB = superDB; part = part2; outerKey; innerKey});
        {
            inner = {canister = Principal.fromActor(inner.canister); key = inner.key};
            outer = {canister = Principal.fromActor(outer.canister); key = outer.key};
        };
    };

    public shared func deleteInner({innerKey: Nac.InnerSubDBKey; sk: Nac.SK}): async () {
        await* Nac.deleteInner({innerSuperDB = superDB; innerKey; sk});
    };

    public query func scanLimitInner({innerKey: Nac.InnerSubDBKey; lowerBound: Nac.SK; upperBound: Nac.SK; dir: BTree.Direction; limit: Nat})
        : async BTree.ScanLimitResult<Text, Nac.AttributeValue>
    {
        // Nac.scanLimitInner({innerSuperDB = superDB; innerKey; lowerBound; upperBound; dir; limit});
    };

    public shared func scanLimitOuter({outerKey: Nac.OuterSubDBKey; lowerBound: Text; upperBound: Text; dir: BTree.Direction; limit: Nat})
        : async BTree.ScanLimitResult<Text, Nac.AttributeValue>
    {
        // await* Nac.scanLimitOuter({outerSuperDB = superDB; outerKey; lowerBound; upperBound; dir; limit});
    };

    public query func scanSubDBs(): async [(Nac.OuterSubDBKey, {canister: Principal; key: Nac.InnerSubDBKey})] {
        // type T1 = (Nac.OuterSubDBKey, Nac.InnerPair);
        type T2 = (Nac.OuterSubDBKey, {canister: Principal; key: Nac.InnerSubDBKey});
        let array: [T1] = Nac.scanSubDBs({superDB});
        let iter = Iter.map(array.vals(), func ((outerKey, v): T1): T2 {
            (outerKey, {canister = Principal.fromActor(v.canister); key = v.key});
        });
        Iter.toArray(iter);
    };

    public query func getByInner({innerKey: Nac.InnerSubDBKey; sk: Nac.SK}): async ?Nac.AttributeValue {
        // Nac.getByInner({superDB; innerKey; sk});
    };

    public query func hasByInner({innerKey: Nac.InnerSubDBKey; sk: Nac.SK}): async Bool {
        // Nac.hasByInner({superDB; innerKey; sk});
    };

    public shared func getByOuter({outerKey: Nac.OuterSubDBKey; sk: Nac.SK}): async ?Nac.AttributeValue {
        await* Nac.getByOuter({outerSuperDB = superDB; outerKey; sk});
    };

    public shared func hasByOuter({outerKey: Nac.OuterSubDBKey; sk: Nac.SK}): async Bool {
        await* Nac.hasByOuter({outerSuperDB = superDB; outerKey; sk});
    };

    public shared func hasSubDBByOuter(options: {outerKey: Nac.OuterSubDBKey}): async Bool {
        await* Nac.hasSubDBByOuter({outerSuperDB = superDB; outerKey = options.outerKey});
    };

    public query func hasSubDBByInner(options: {innerKey: Nac.InnerSubDBKey}): async Bool {
        // Nac.hasSubDBByInner({innerSuperDB = superDB; innerKey = options.innerKey});
    };

    public shared func subDBSizeByOuter({outerKey: Nac.OuterSubDBKey}): async ?Nat {
        await* Nac.subDBSizeByOuter({outerSuperDB = superDB; outerKey});
    };

    public query func subDBSizeByInner({innerKey: Nac.InnerSubDBKey}): async ?Nat {
        // Nac.subDBSizeByInner({superDB; innerKey});
    };

    public shared func startInsertingImpl({
        innerKey: Nac.InnerSubDBKey;
        sk: Nac.SK;
        value: Nac.AttributeValue;
        // needsMove: Bool;
    }): async () {
        // let index: Nac.IndexCanister = actor(Principal.toText(indexCanister));
        // let outer: Nac.OuterCanister = actor(Principal.toText(outerCanister));
        await* Nac.startInsertingImpl({
            innerKey;
            sk;
            value;
            innerSuperDB = superDB;
        });
    };

    public shared func getSubDBUserDataInner(options: {innerKey: Nac.InnerSubDBKey}) : async ?Text {
        Nac.getSubDBUserDataInner({superDB; subDBKey = options.innerKey});
    };

    public shared func deleteSubDBOuter({outerKey: Nac.OuterSubDBKey}) : async () {
        await* Nac.deleteSubDBOuter({superDB; outerKey});
    };

    // public shared func hasByOuterPartitionKey(options: Nac.HasByOuterPartitionKeyOptions) : async Bool {
    //     await* Nac.hasByOuterPartitionKey(options);
    // };

    public shared func getSubDBUserDataOuter(options: {outerKey: Nac.OuterSubDBKey}) : async ?Text {
        await* Nac.getSubDBUserDataOuter({outerSuperDB = superDB; outerKey = options.outerKey});
    };

    public shared func subDBSizeOuterImpl(options: {outerKey: Nac.OuterSubDBKey}): async ?Nat {
        await* Nac.subDBSizeOuterImpl({outerSuperDB = superDB; outerKey = options.outerKey}, Common.dbOptions);
    };

    /// Cycles ///

    public query func cycles_simple_availableCycles(): async Nat {
        Cycles.available();
    };

    public shared func cycles_simple_topUpCycles: (cycles: Nat) -> /*async*/ () {
        Cycles.accept(cycles);
    };
}