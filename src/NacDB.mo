import Principal "mo:base/Principal";
import BTree "mo:btree/BTree";
import RBT "mo:stable-rbtree/StableRBTree";
import Text "mo:base/Text";
import Nat "mo:base/Nat";

module {
    public type PK = Principal;

    public type SubDBKey = Nat;

    public type SK = Text;

    public type AttributeKey = Text;

    public type AttributeValuePrimitive = {#text : Text; #int : Int; #bool : Bool; #float : Float};
    public type AttributeValueBlob = {#blob : Blob};
    public type AttributeValueTuple = {#tuple : [AttributeValuePrimitive]};
    public type AttributeValueArray = {#arrayText : [Text]; #arrayInt : [Int]; #arrayBool : [Bool]; #arrayFloat : [Float]};
    public type AttributeValueRBTreeValue = AttributeValuePrimitive or AttributeValueBlob or AttributeValueTuple or AttributeValueArray;
    public type AttributeValueRBTree = {#tree : RBT.Tree<Text, AttributeValueRBTreeValue>};
    public type AttributeValue = AttributeValuePrimitive or AttributeValueBlob or AttributeValueTuple or AttributeValueArray or AttributeValueRBTree;

    // TODO: Max items per DB with auto removal of loosers.
    type SubDB = {
        // pk: PK;
        // subDBKey: SubDBKey;
        data: BTree.BTree<SK, AttributeValue>;
        inMoving: Bool; // While moving to another canister, write operations are disabled.
        hardCap: Nat; // Remove looser items after reaching this count.
    };

    type MoveCap = { #numDBs: Nat; #usedMemory: Nat };

    type SuperDB = {
        subDBs: BTree.BTree<SubDBKey, SubDB>;
        moveCap: MoveCap;
        moveCallback: ?(shared (oldPK: PK, oldSubDBKey: SubDBKey, newPK: PK, newSubDBKey: SubDBKey) -> ());
    };

    public func getSubDB(superDB: SuperDB, subDBKey: SubDBKey) : ?SubDB {
        BTree.get<SubDBKey, SubDB>(superDB.subDBs, Nat.compare, subDBKey);
    };

    public type GetOptions = {subDB: SubDB; sk: SK};

    public func get(options: GetOptions) : ?AttributeValue {
        BTree.get(options.subDB.data, Text.compare, options.sk);
    };

    public type ExistsOptions = GetOptions;

    public func skExists(options: ExistsOptions) : Bool {
        BTree.has(options.subDB.data, Text.compare, options.sk);
    };

// func moveSubDB
};