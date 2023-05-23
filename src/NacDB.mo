import Principal "mo:base/Principal";
import BTree "mo:btree/BTree";
import RBT "mo:stable-rbtree/StableRBTree";
import Nat32 "mo:base/Nat32";
import Text "mo:base/Text";

module {
    public type PK = Principal;

    public type SubDBKey = Nat32;

    public type SK = Text;

    public type AttributeKey = Text;

    public type AttributeValuePrimitive = {#text : Text; #int : Int; #bool : Bool; #float : Float};
    public type AttributeValueBlob = {#blob : Blob};
    public type AttributeValueTuple = {#tuple : [AttributeValuePrimitive]};
    public type AttributeValueArray = {#arrayText : [Text]; #arrayInt : [Int]; #arrayBool : [Bool]; #arrayFloat : [Float]};
    public type AttributeValueRBTreeValue = AttributeValuePrimitive or AttributeValueBlob or AttributeValueTuple or AttributeValueArray;
    public type AttributeValueRBTree = {#tree : RBT.Tree<Text, AttributeValueRBTreeValue>};
    public type AttributeValue = AttributeValuePrimitive or AttributeValueBlob or AttributeValueTuple or AttributeValueArray or AttributeValueRBTree;

    // TODO: Max items per DB with auto removal of losers.
    type SubDB = {
        // pk: PK;
        // subDBKey: SubDBKey;
        data: BTree.BTree<SK, AttributeValue>;
        inMoving: Bool; // While moving to another canister, write operations are disabled.
    };

    type SuperDB = {
        subDBs: BTree.BTree<SubDBKey, SubDB>;
    };

    public func getSubDB(superDB: SuperDB, subDBKey: SubDBKey) : ?SubDB {
        BTree.get<SubDBKey, SubDB>(superDB.subDBs, Nat32.compare, subDBKey);
    };

    public type GetOptions = {subDB: SubDB; sk: SK};

    public func get(options: GetOptions) : ?AttributeValue {
        BTree.get(options.subDB.data, Text.compare, options.sk);
    };

    public type ExistsOptions = GetOptions;

    public func skExists(options: ExistsOptions) : Bool {
        BTree.has(options.subDB.data, Text.compare, options.sk);
    };
};