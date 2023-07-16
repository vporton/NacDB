import Cycles "mo:base/ExperimentalCycles";
import Nac "../../../src/NacDB";
import Index "../index/main";
import Partition "../partition/main";
import Debug "mo:base/Debug";

actor {
    let moveCap = #usedMemory 500_000;
    let dbOptions = {moveCap; hardCap = ?1000; newPartitionCycles = 300_000_000_000};

    stable var index : ?Index.Index = null;
    stable var location: ?(Nac.PartitionCanister, Nac.OuterSubDBKey) = null;

    public shared func init() : async () {
        Cycles.add(700_000_000_000);
        let index0 = await Index.Index(dbOptions);
        index := ?index0;
        await index0.init();
    };

    public shared func greet(name : Text) : async Text {
        let ?index0 = index else {
          Debug.trap("no index canister")
        };
        let location0 = await index0.createSubDB({dbOptions : Nac.DBOptions; creatingId = insertId; index = index0});
        location := ?location0;
        let (part, subDBKey) = location0;
        let (part2, subDBKey2) = await part.insert({dbOptions; index = index0; subDBKey = subDBKey; sk = "name"; value = #text name});
        let name2 = await part2.get({subDBKey = subDBKey2; sk = "name"});
        let ?#text name3 = name2 else {
          Debug.trap("error");
        };

        return "Hello, " # name3 # "!";
    };
};
