import Cycles "mo:base/ExperimentalCycles";
import Principal "mo:base/Principal";
import HashMap "mo:base/HashMap";
import Debug "mo:base/Debug";

module {
  public type CanisterFulfillmentInfo = {
    threshold: Nat;
    installAmount: Nat;
  };

  /// It makes sense to provide only, if the battery is controlled by childs.
  public type BatteryActor = actor {
    cycles_simple_provideCycles: query (needy: Principal) -> async ();
  };

  public type ChildActor = actor {
    cycles_simple_availableCycles: query () -> async Nat;
  };

  public type CanisterKind = Text;

  public type CanisterMap = HashMap.HashMap<Principal, CanisterKind>;

  public type CanisterKindsMap = HashMap.HashMap<CanisterKind, CanisterFulfillmentInfo>;

  /// Battery API ///

  public type Battery = {
    canisterMap: CanisterMap;
    canisterKindsMap: CanisterKindsMap;
  };

  public func topUpOneCanister(battery: Battery, canisterId: Principal): async* () {
    let info0 = do ? { battery.canisterKindsMap.get(battery.canisterMap.get(canisterId)!)! };
    let ?info = info0 else {
      Debug.trap("no such canister record");
    };
    let child: ChildActor = actor(Principal.toText(canisterId));
    let remaining = await child.cycles_simple_availableCycles();
    if (remaining <= info.threshold) {
      Cycles.add<system>(info.installAmount);
      let ic : actor {
        deposit_cycles : shared { canister_id : Principal } -> async ();
      } = actor ("aaaaa-aa");
      await ic.deposit_cycles({canister_id = canisterId});
    };
  };

  public func topUpAllCanisters(battery: Battery): async* () {
    for (canisterId in battery.canisterMap.keys()) {
      await* topUpOneCanister(battery, canisterId);
    };
  };

  public func addCanister(battery: Battery, canisterId: Principal, kind: Text) {
    battery.canisterMap.put(canisterId, kind);
  };

  public func insertCanisterKind(battery: Battery, kind: Text, info: CanisterFulfillmentInfo) {
    battery.canisterKindsMap.put(kind, info);
  };

  /// ChildActor API ///

  public func askForCycles(batteryPrincipal: Principal, needy: Principal, threshold: Nat): async* () {
    if (Cycles.available() < threshold) {
      let battery: BatteryActor = actor(Principal.toText(batteryPrincipal));
      await battery.cycles_simple_provideCycles(needy);
    };
  };
};
