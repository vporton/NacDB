import Cycles "mo:base/ExperimentalCycles";
import Int "mo:base/Int";
import Debug "mo:base/Debug";

module {
    public func topUpCycles(): (accepted : Nat) {
        Debug.print("accepting " # debug_show(Cycles.available() / 2) # " cycles");
        // FIXME: Fails if called twice:
        Cycles.accept(Cycles.available() / 2); // TODO: refuse if we already have enough
    };

    // FIXME: Wrong for main canister.
    public func addPart() {
        Debug.print("adding " # debug_show(Cycles.balance() / 2) # " cycles");
        Cycles.add(Cycles.balance() / 2);
    };
}