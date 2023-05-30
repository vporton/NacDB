import Index "../index/main";
import Partition "../partition/main";
import Debug "mo:base/Debug";

actor {
  public shared func greet(name : Text) : async Text {
    let index = await Index.Index();
    await index.init();
    
    let (part, subDBKey) = await index.insertSubDB();
    await part.insert({subDBKey; sk = "name"; value = #text name});
    let name2 = await part.get({subDBKey; sk = "name"});
    let ?#text name3 = name2 else {
      Debug.trap("error");
    };

    return "Hello, " # name3 # "!";
  };
};
