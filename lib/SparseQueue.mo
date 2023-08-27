import RBT "mo:stable-rbtree/StableRBTree";
import Debug "mo:base/Debug";
import Nat "mo:base/Nat";
import Blob "mo:base/Blob";
import Bool "mo:base/Bool";

/// The intended use is a queue of operations on a canister.
/// `maxSize` protects against memory overflow.
module {
    public type GUID = Blob;

    // FIXME: RBT leaves memory allocated for deleted items.
    // FIXME: Remove old (by time) items.
    public type SparseQueue<T> = {
        var tree: RBT.Tree<GUID, (Nat, T)>;
        var order: RBT.Tree<Nat, GUID>; // TODO: this variable unneeded
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
            case (?v) return v.1; // already there is
            case (null) {};
        };
        if (RBT.size(queue.order) >= queue.maxSize) {
            Debug.print("QUEUE OVERFLOW");
            Debug.trap("QUEUE OVERFLOW");
            // let i = RBT.iter(queue.order, #fwd);
            // let ?(number, _) = i.next() else {
            //     Debug.trap("empty queue");
            // };
            // let ?guid2 = RBT.get(queue.order, Nat.compare, number) else {
            //     Debug.trap("programming error")
            // };
            // queue.order := RBT.delete(queue.order, Nat.compare, number);
            // queue.tree := RBT.delete<Blob, T>(queue.tree, Blob.compare, guid2);
        };
        let k = queue.next;
        queue.tree := RBT.put(queue.tree, Blob.compare, guid, (k, value));
        queue.order := RBT.put(queue.order, Nat.compare, k, guid);
        queue.next += 1;
        value;
    };

    public func delete<T>(queue: SparseQueue<T>, key: GUID) {
        let v0 = RBT.get(queue.tree, Blob.compare, key);
        let ?v = v0 else {
            Debug.trap("programming error");
        };
        queue.order := RBT.delete(queue.order, Nat.compare, v.0);
        queue.tree := RBT.delete(queue.tree, Blob.compare, key);
    };

    public func get<T>(queue: SparseQueue<T>, key: GUID): ?T {
        do ? { RBT.get(queue.tree, Blob.compare, key)!.1 };
    };
}