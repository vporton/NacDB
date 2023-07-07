import RBT "mo:stable-rbtree/StableRBTree";
import CA "mo:candb/CanisterActions";
import CanDB "mo:candb/CanDB";
import Entity "mo:candb/Entity";
import Principal "mo:base/Principal";
import Debug "mo:base/Debug";
import Text "mo:base/Text";

shared actor class UserCanister({
  partitionKey: Text;
  scalingOptions: CanDB.ScalingOptions;
}) {

  /// @required (may wrap, but must be present in some form in the canister)
  ///
  /// Initialize CanDB
  stable let db = CanDB.init({
    pk = partitionKey;
    scalingOptions = scalingOptions;
    btreeOrder = null;
  });

  /// @recommended (not required) public API
  public query func getPK(): async Text { db.pk };

  /// @required public API (Do not delete or change)
  public query func skExists(sk: Text): async Bool { 
    CanDB.skExists(db, sk);
  };

  /// @required public API (Do not delete or change)
  public shared({ caller = caller }) func transferCycles(): async () {
    await CA.transferCycles(caller);
  };

  public func put(sk: Text, value: {canister: Principal; key: Nat}): async () {
    await* CanDB.put(db, {
      sk = sk;
      attributes = [
        ("part", #text(Principal.toText(value.canister))),
        ("key", #int(value.key)),
      ];
    });
  };

  public func get(sk: Text): async {canister: Principal; key: Nat} {
    let ?value = CanDB.get(db, {
      sk = sk;
    }) else {
        Debug.trap("reference does not exist");
    };
    let ?#text part = RBT.get(value.attributes, Text.compare, "part") else {
        Debug.trap("wrong format");
    };
    let ?#num key = RBT.get(value.attributes, Text.compare, "key") else {
        Debug.trap("wrong format");
    };
    {canister = Principal.fromText(part); key = key};
  };
}