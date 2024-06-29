import BTree "mo:stableheapbtreemap/BTree";
import Nac "../../../src/NacDB";
import Principal "mo:base/Principal";
import Bool "mo:base/Bool";
import Nat "mo:base/Nat";
import Cycles "mo:base/ExperimentalCycles";
import MyCycles "mo:cycles-simple";
import Text "mo:base/Text";
import Iter "mo:base/Iter";
import Common "../common";

shared({caller}) actor class Partition() = this {
    public composite query func scanLimitOuterComposite({outerKey: Nac.OuterSubDBKey; lowerBound: Text; upperBound: Text; dir: BTree.Direction; limit: Nat})
        : async BTree.ScanLimitResult<Text, Nac.AttributeValue>
    {
        await* Nac.scanLimitOuter({outerSuperDB = superDB; outerKey; lowerBound; upperBound; dir; limit});
    };
}