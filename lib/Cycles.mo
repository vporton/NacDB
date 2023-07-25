import Cycles "mo:base/ExperimentalCycles";
import Int "mo:base/Int";

module {
    public func topUpCycles(amount: Nat): (accepted : Nat) {
        let diff = Int.max(0, amount - Cycles.balance());
        Cycles.accept(Int.abs(diff));
    }
}