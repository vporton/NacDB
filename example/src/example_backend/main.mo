import Cycles "mo:base/ExperimentalCycles";
import Index "../index/main";
import Partition "../partition/main";
import Debug "mo:base/Debug";

actor {
  var index : ?Index.Index = null;

  public shared func init() : async () {
    Cycles.add(700_000_000_000);
    let index0 = await Index.Index();
    index := ?index0;
    await index0.init();
  };

  public shared func greet(name : Text) : async Text {
    let ?index0 = index else {
      Debug.trap("no index canister")
    };
    let (part, subDBKey) = await index0.insertSubDB();
    await part.insert({subDBKey; sk = "name"; value = #text name});
    let name2 = await part.get({subDBKey; sk = "name"});
    let ?#text name3 = name2 else {
      Debug.trap("error");
    };

    return "Hello, " # name3 # "!";
  };
};
