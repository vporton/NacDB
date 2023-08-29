import Cycles "mo:base/ExperimentalCycles";
import Nac "../../../src/NacDB";
import GUID "../../../lib/GUID";
import MyCycles "../../../lib/Cycles";
import Index "../index/main";
import Partition "../partition/main";
import Debug "mo:base/Debug";
import Array "mo:base/Array";

actor {
    // TODO: https://forum.dfinity.org/t/why-is-actor-class-constructor-not-shared/21424
    public shared func constructor(dbOptions: Nac.DBOptions): async Partition.Partition {
        await Partition.Partition(dbOptions);
    };

    let dbOptions = {
        moveCap = #usedMemory 500_000;
        hardCap = ?1000;
        partitionCycles = 10_000_000_000;
        constructor = constructor;
        timeout = 20 * 1_000_000_000; // 20 sec
    };

    stable var index : ?Index.Index = null;

    public shared func init() : async () {
        MyCycles.addPart(dbOptions.partitionCycles);
        let index0 = await Index.Index(dbOptions);
        index := ?index0;
        MyCycles.addPart(dbOptions.partitionCycles);
        await index0.init();
    };

    public shared func greet(name : Text) : async Text {
        let ?index0 = index else {
          Debug.trap("no index canister")
        };
        let guidGen = GUID.init(Array.tabulate<Nat8>(16, func _ = 0));
        let location = await index0.createSubDB({guid = GUID.nextGuid(guidGen); dbOptions; index = index0; userData = ""});
        let {outer = (part, subDBKey)} = location;
        let {outer = (part2, subDBKey2)} = await part.insert({
            guid = GUID.nextGuid(guidGen);
            dbOptions;
            indexCanister = index0;
            outerCanister = part;
            outerKey = subDBKey;
            sk = "name";
            value = #text name;
        });
        let name2 = await part2.getByOuter({subDBKey = subDBKey2; sk = "name"});
        let ?#text name3 = name2 else {
          Debug.trap("error");
        };

        return "Hello, " # name3 # "!";
    };
};
