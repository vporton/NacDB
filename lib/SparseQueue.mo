import RBT "mo:stable-rbtree/StableRBTree";
import Debug "mo:base/Debug";
import Nat "mo:base/Nat";
import Blob "mo:base/Blob";
import Bool "mo:base/Bool";

/// The intended use is a queue of operations on a canister.
/// `maxSize` protects against memory overflow.
module {
    public type GUID = Blob;

    public type SparseQueue<T> = {
        var tree: RBT.Tree<GUID, T>;
        var order: RBT.Tree<Nat, GUID>;
        maxSize: Nat;
        var next: Nat;
    };

    public func init<T>(maxSize: Nat): SparseQueue<T> {
        {
            var tree = RBT.init();
            var order = RBT.init();
            maxSize;
            var next = 0;
        }
    };

    /// It returns `value` or an old value. TODO: It is error prone.
    public func add<T>(queue: SparseQueue<T>, guid: GUID, value: T): T {
        switch (RBT.get(queue.tree, Blob.compare, guid)) {
            case (?v) return v; // already there is
            case (null) {};
        };
        if (RBT.size(queue.order) >= queue.maxSize) {
            Debug.print("QUEUE OVERFLOW");
            let i = RBT.iter(queue.order, #fwd);
            let ?(number, _) = i.next() else {
                Debug.trap("empty queue");
            };
            let ?guid2 = RBT.get(queue.order, Nat.compare, number) else {
                Debug.trap("programming error")
            };
            queue.order := RBT.delete(queue.order, Nat.compare, number);
            queue.tree := RBT.delete<Blob, T>(queue.tree, Blob.compare, guid2);
        };
        queue.tree := RBT.put(queue.tree, Blob.compare, guid, value);
        let k = queue.next;
        queue.order := RBT.put(queue.order, Nat.compare, k, guid);
        queue.next += 1;
        value;
    };

    // We don't really need this.
    // public func delete<T>(queue: SparseQueue<T>, key: GUID) {
    //     queue.tree := RBT.delete(queue.tree, Nat.compare, key);
    // };

    public func get<T>(queue: SparseQueue<T>, key: GUID): ?T {
        RBT.get(queue.tree, Blob.compare, key);
    };
}