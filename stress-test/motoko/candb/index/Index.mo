import Principal "mo:base/Principal";
import Debug "mo:base/Debug";
import Cycles "mo:base/ExperimentalCycles";
import Text "mo:base/Text";
import Buffer "mo:stable-buffer/StableBuffer";

import UserCanister "../partition/Partition";

import CanisterMap "mo:candb/CanisterMap";
import CA "mo:candb/CanisterActions";
import Admin "mo:candb/CanDBAdmin";
import Utils "mo:candb/Utils";

shared actor class IndexCanister() = this {
  stable var pkToCanisterMap = CanisterMap.init();

  /// @required API (Do not delete or change)
  ///
  /// Get all canisters for an specific PK
  ///
  /// This method is called often by the candb-client query & update methods. 
  public shared query({caller = caller}) func getCanistersByPK(pk: Text): async [Text] {
    getCanisterIdsIfExists(pk);
  };
  
  func createCanister(pk: Text): async Text {
    Cycles.add(300_000_000_000);
    let newUserCanister = await UserCanister.UserCanister({
      partitionKey = pk;
      scalingOptions = {
        autoScalingHook = autoScaleUserCanister;
        sizeLimit = #count 50000;
      };
    });
    let newUserCanisterPrincipal = Principal.fromActor(newUserCanister);
    // await CA.updateCanisterSettings({
    //   canisterId = newUserCanisterPrincipal;
    //   settings = {
    //     controllers = [caller];
    //     compute_allocation = ?0;
    //     memory_allocation = ?0;
    //     freezing_threshold = ?2592000;
    //   }
    // });

    let newUserCanisterId = Principal.toText(newUserCanisterPrincipal);
    pkToCanisterMap := CanisterMap.add(pkToCanisterMap, pk, newUserCanisterId);

    newUserCanisterId;
  };

  /// This hook is called by CanDB for AutoScaling the User Service Actor.
  ///
  /// If the developer does not spin up an additional User canister in the same partition within this method, auto-scaling will NOT work
  public shared func autoScaleUserCanister(pk: Text): async Text {
    // Auto-Scaling Authorization - ensure the request to auto-scale the partition is coming from an existing canister in the partition, otherwise reject it
    await createCanister(pk);
  };
  
  /// @required function (Do not delete or change)
  ///
  /// Helper method acting as an interface for returning an empty array if no canisters
  /// exist for the given PK
  func getCanisterIdsIfExists(pk: Text): [Text] {
    switch(CanisterMap.get(pkToCanisterMap, pk)) {
      case null { [] };
      case (?canisterIdsBuffer) { Buffer.toArray(canisterIdsBuffer) } 
    }
  };
}