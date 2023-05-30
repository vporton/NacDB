import Index "../index";
import Partition "../partition";

actor {
  public query func greet(name : Text) : async Text {
    Index.insertSubDB();
    return "Hello, " # name # "!";
  };
};
