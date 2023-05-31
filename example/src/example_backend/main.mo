import Index "../index/main";
import Partition "../partition/main";
import Debug "mo:base/Debug";
import Cycles "mo:base/ExperimentalCycles";

actor {
  var index: ?Index.Index = null;
  public shared func init() : async () {
    Cycles.add(290_000_000_000);
    let index0 = await Index.Index();
    await index0.init();
    index := ?index0;
  };
  public shared func greet(name : Text) : async Text {
    let ?index0 = index else {
      Debug.trap("index not initialized")
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
