import Cycles "mo:base/ExperimentalCycles";
import Nac "../../../src/NacDB";
import Index "../index/main";
import Partition "../partition/main";
import Debug "mo:base/Debug";

actor {
  stable var index : ?Index.Index = null;
  stable var location: ?(Nac.PartitionCanister, Nac.SubDBKey) = null;

  public shared func movingCallback({
      oldCanister: Nac.PartitionCanister;
      oldSubDBKey: Nac.SubDBKey;
      newCanister: Nac.PartitionCanister;
      newSubDBKey: Nac.SubDBKey;
  }) : async ()
  {
      location := ?(newCanister, newSubDBKey);
  };

  public shared func init() : async () {
    Cycles.add(700_000_000_000);
    let index0 = await Index.Index();
    index := ?index0;
    await index0.init(?movingCallback);
  };

  public shared func greet(name : Text) : async Text {
    let ?index0 = index else {
      Debug.trap("no index canister")
    };
    let insertId = await index0.startInsertingSubDB();
    let location0 = await index0.finishInsertingSubDB(insertId);
    location := ?location0;
    let (part, subDBKey) = location0;
    await part.insert({subDBKey = subDBKey; sk = "name"; value = #text name});
    await part.finishMovingSubDB({index; superDB; dbOptions});
    let name2 = await part.get({subDBKey; sk = "name"});
    let ?#text name3 = name2 else {
      Debug.trap("error");
    };

    return "Hello, " # name3 # "!";
  };
};
