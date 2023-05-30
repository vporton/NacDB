import Index "../index";
import Partition "../partition";

actor {
  public shared func greet(name : Text) : async Text {
    let index = await Index.Index();
    await index.init();
    
    let (part, subDBKey) = await index.insertSubDB();
    part.insert({subDBKey; sk = "name"; value = #text name});
    let name2 = part.get({subDBKey; sk = "name"});

    return "Hello, " # name2 # "!";
  };
};
