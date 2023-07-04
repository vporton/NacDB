import RBT "mo:stable-rbtree/StableRBTree";
import Debug "mo:base/Debug";
import Nat "mo:base/Nat";

/// The intended use is a queue of operations on a canister.
/// `maxSize` protects against memory overflow.
module {
    public type SparseQueueKey = Nat;

    public type SparseQueue<T> = {
        var tree: RBT.Tree<SparseQueueKey, T>;
        maxSize: Nat;
        var next: SparseQueueKey;
    };

    public func init<T>(maxSize: Nat): SparseQueue<T> {
        {
            var tree = RBT.init();
            maxSize;
            var next = 0;
        }
    };

    public func add<T>(queue: SparseQueue<T>, value: T): SparseQueueKey {
        if (RBT.size(queue.tree) >= queue.maxSize) {
            let i = RBT.iter(queue.tree, #fwd);
            let ?(key, _) = i.next() else {
                Debug.trap("empty queue");
            };
            queue.tree := RBT.delete(queue.tree, Nat.compare, key);
        };
        queue.tree := RBT.put(queue.tree, Nat.compare, queue.next, value);
        let k = queue.next;
        queue.next += 1;
        k;
    };

    public func delete<T>(queue: SparseQueue<T>, key: SparseQueueKey) {
        queue.tree := RBT.delete(queue.tree, Nat.compare, key);
    };

    public func get<T>(queue: SparseQueue<T>, key: SparseQueueKey): ?T {
        RBT.get(queue.tree, Nat.compare, key);
    };
}