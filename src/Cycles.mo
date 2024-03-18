import Cycles "mo:base/ExperimentalCycles";
import Int "mo:base/Int";
import Nat "mo:base/Nat";

module {
    /// Accept (enough) cycles (up to `maxCycles`) for the current actor to act.
    ///
    /// Returns the amount of cycles accepted.
    public func topUpCycles<system>(maxCycles: Nat): (accepted : Nat) {
        // Debug.print("maxCycles: " # debug_show(maxCycles) # " Proposed cycles: " # debug_show(Cycles.available()) # " balance: " # debug_show(Cycles.balance()));
        // let amount = Int.min(maxCycles, Int.max(0, Cycles.available() + Cycles.balance()));
        let amount = Int.min(maxCycles, Cycles.available()) - Cycles.balance();
        let amount2 = Int.max(amount, 0);
        // Debug.print("Accepting cycles: " # debug_show(amount2));
        Cycles.accept<system>(Int.abs(amount2));
    };

    /// Send up to `maxAmount` cycles to the next `await` call.
    public func addPart<system>(maxAmount: Nat) {
        let amount = Nat.min(Cycles.balance() / 2, maxAmount);
        ignore Cycles.accept<system>(amount);
        // Debug.print("adding " # debug_show(amount) # " cycles");
        Cycles.add<system>(amount);
    };
}