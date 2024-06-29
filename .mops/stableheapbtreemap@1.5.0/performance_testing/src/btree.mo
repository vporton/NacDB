import Nat "mo:base/Nat";
import Iter "mo:base/Iter";
import Prim "mo:prim";
import BT "mo:btree/BTree";
import Check "mo:btree/Check";
import Debug "mo:base/Debug";

import Time "mo:base/Time";


actor Echo {
  stable var stableBTree = BT.init<Nat, Nat>(?128);
  stable var itemCount = 0;

  public func testBTree(items : Nat) : async Nat {
    // max around 246000

    let tree = BT.init<Nat, Nat>(?4);

    for (item in Iter.range(0, items)) {
      ignore BT.insert<Nat, Nat>(tree, Nat.compare, item, item);
    };

    return items;
  };

  public func now() : async Int {
    Time.now()
  };



  //////////////////////////////////////////////////////////////////////////////////////////////////

  public func getStableStateSize() : async Nat {
    let result = Prim.rts_heap_size();
    return result;
  };

  public func getItemCount(): async Nat {
    return itemCount;
  };

  public func size(): async Nat {
    return stableBTree.size
  };

  public func allocateBTSpace(count: Nat) : async Nat {
    let end = itemCount + count;

    for (item in Iter.range(itemCount, end)) {
      Debug.print("item" # Nat.toText(item));
      ignore BT.insert<Nat, Nat>(stableBTree, Nat.compare, item, item);
    };
    itemCount := end;
    return end;
  };

  public func clearWithOrder(order: ?Nat): () {
    stableBTree := BT.init<Nat, Nat>(order);
    itemCount := 0;
  };

  public func checkTreeDepth(): async Check.CheckDepthResult {
    Check.checkTreeDepthIsValid<Nat, Nat>(stableBTree)
  };


  public func getValueForKey(key: Nat): async ?Nat {
    BT.get<Nat, Nat>(stableBTree, Nat.compare, key)
  };


  /*
  public func getRBTreeItem(k: Text): async ?Nat {
    BT.get(stableRBTree, k);
  };
  */
};