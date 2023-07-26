import Cycles "mo:base/ExperimentalCycles";
import Int "mo:base/Int";
import Debug "mo:base/Debug";
import Nat "mo:base/Nat";

module {
    public func topUpCycles(): (accepted : Nat) {
        // Debug.print("accepting " # debug_show(Cycles.available() / 2) # " cycles");
        let amount = Int.max(0, Cycles.available() - Cycles.balance());
        Cycles.accept(Int.abs(amount));
    };

    // FIXME: Wrong for main canister.
    public func addPart(maxAmount: Nat) {
        // Debug.print("adding " # debug_show(Cycles.balance() / 2) # " cycles");
        let amount = Nat.max(Cycles.balance() / 2, maxAmount);
        Cycles.add(amount);
    };
}