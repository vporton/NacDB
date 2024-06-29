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

        /// Retrieve sub-DB entries by its outer key.
        public func scanLimitOuter(options: ScanLimitOuterOptions): async* RBT.ScanLimitResult<Text, AttributeValue> {
            let ?{canister = part; key = innerKey} = getInner({outerKey = options.outerKey; superDB = options.outerSuperDB}) else {
                Debug.trap("no sub-DB");
            };
            await part.scanLimitInner({innerKey; lowerBound = options.lowerBound; upperBound = options.upperBound; dir = options.dir; limit = options.limit});
        };
    };
}