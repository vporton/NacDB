import Cycles "mo:base/ExperimentalCycles";
import Nac "../../../src/NacDB";
import GUID "../../../src/GUID";
import MyCycles "mo:cycles-simple";
import Index "../index/main";
import Partition "../partition/main";
import Debug "mo:base/Debug";
import Array "mo:base/Array";
import Blob "mo:base/Blob";
import Principal "mo:base/Principal";
import Common "../common";

actor {

    stable var index : ?Index.Index = null;

    public shared func init() : async () {
        ignore MyCycles.topUpCycles(Common.dbOptions.partitionCycles);
        MyCycles.addPart(Common.dbOptions.partitionCycles);
        let index0 = await Index.Index();
        index := ?index0;
        MyCycles.addPart(Common.dbOptions.partitionCycles);
        await index0.init();
    };

    public shared func greet(name : Text) : async Text {
        let ?index0 = index else {
          Debug.trap("no index canister")
        };
        let guidGen = GUID.init(Array.tabulate<Nat8>(16, func _ = 0));
        let location = await index0.createSubDB(Blob.toArray(GUID.nextGuid(guidGen)), {index = index0; userData = ""; hardCap = ?1000});
        let {outer = (part, subDBKey)} = location;
        let partx: Nac.PartitionCanister = actor(Principal.toText(part));
        let {outer = (part2, subDBKey2)} = await partx.insert({
            guid = Blob.toArray(GUID.nextGuid(guidGen));
            outerCanister = partx;
            outerKey = subDBKey;
            sk = "name";
            value = #text name;
        });
        let name2 = await part2.getByOuter({outerKey = subDBKey2; sk = "name"});
        let ?#text name3 = name2 else {
          Debug.trap("error");
        };

        return "Hello, " # name3 # "!";
    };
};
