import Cycles "mo:base/ExperimentalCycles";
import Int "mo:base/Int";
import Debug "mo:base/Debug";
import Nat "mo:base/Nat";

module {
    public func topUpCycles(maxCycles: Nat): (accepted : Nat) {
        // Debug.print("maxCycles: " # debug_show(maxCycles) # " Proposed cycles: " # debug_show(Cycles.available()) # " balance: " # debug_show(Cycles.balance()));
        let amount = Int.min(maxCycles, Int.max(0, Cycles.available() + Cycles.balance()));
        // Debug.print("Accepting cycles: " # debug_show(amount));
        Cycles.accept(Int.abs(amount));
    };

    // FIXME: Wrong for main canister.
    public func addPart(maxAmount: Nat) {
        // Debug.print("adding " # debug_show(Cycles.balance() / 2) # " cycles");
        let amount = Nat.min(Cycles.balance() / 3, maxAmount); // FIXME: `/ 3` is a hack.
        Cycles.add(amount); // FIXME: `* 10` is a hack.
    };
}