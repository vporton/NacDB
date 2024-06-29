import Nac "NacDB";
import BTree "mo:stableheapbtreemap/BTree";
import RBT "mo:stable-rbtree/StableRBTree";

shared({caller}) actor class Partition() = this {
    public composite query func scanLimitOuterComposite({outerKey: Nac.OuterSubDBKey; lowerBound: Text; upperBound: Text; dir: BTree.Direction; limit: Nat})
        : async BTree.ScanLimitResult<Text, Nac.AttributeValue>
    {
        await* N.scanLimitOuter({outerSuperDB = superDB; outerKey; lowerBound; upperBound; dir; limit});
    };

    type ScanLimitOuterOptions = {outerSuperDB: Nac.SuperDB; outerKey: Nac.OuterSubDBKey; lowerBound: Text; upperBound: Text; dir: RBT.Direction; limit: Nat};

    module N {
        type AttributeValue = Nac.AttributeValue;

        type Test = actor {
            scanLimitInner: query({innerKey: Nac.InnerSubDBKey; lowerBound: Nac.SK; upperBound: Nac.SK; dir: RBT.Direction; limit: Nat})
                -> async RBT.ScanLimitResult<Text, AttributeValue>;

        };

        /// Retrieve sub-DB entries by its outer key.
        public func scanLimitOuter(options: ScanLimitOuterOptions): async* RBT.ScanLimitResult<Text, AttributeValue> {
            let part: Test = actor("aaaaa-aa");
            await part.scanLimitInner({innerKey; lowerBound = options.lowerBound; upperBound = options.upperBound; dir = options.dir; limit = options.limit});
        };
    };
}