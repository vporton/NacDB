import Iter "mo:base/Iter";
import Debug "mo:base/Debug";
import Array "mo:base/Array";

actor StressTest {
  
    public func main() {
        let nThreads = 3;
        let threads : [var ?(async())] = Array.init(nThreads, null);
        for (i in threads.keys()) {
            threads[i] := ?runThread();
        };     
        for (topt in threads.vals()) {
            let ?t = topt else {
                Debug.trap("programming error");
            };
            await t;
        }
    };

    func runThread() : async () {
        Debug.print("XXX");
    }
}