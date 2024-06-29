import Nac "NacDB";
import BTree "mo:stableheapbtreemap/BTree";

shared({caller}) actor class Partition() = this {
    public composite query func scanLimitOuterComposite({outerKey: Nac.OuterSubDBKey; lowerBound: Text; upperBound: Text; dir: BTree.Direction; limit: Nat})
        : async BTree.ScanLimitResult<Text, Nac.AttributeValue>
    {
        await* Nac.scanLimitOuter({outerSuperDB = superDB; outerKey; lowerBound; upperBound; dir; limit});
    };
}