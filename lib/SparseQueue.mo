import Debug "mo:base/Debug";
import Nat "mo:base/Nat";
import Blob "mo:base/Blob";
import Bool "mo:base/Bool";
import Time "mo:base/Time";
import Int "mo:base/Int";
import BTree "mo:btree/BTree";

/// The intended use is a queue of operations on a canister.
/// `maxSize` protects against memory overflow.
module {
    public type GUID = Blob;

    // FIXME: Remove old (by time) items.
    public type SparseQueue<T> = {
        var tree: BTree.BTree<GUID, (Nat, T)>;
        var order: BTree.BTree<Time.Time, GUID>;
        maxSize: Nat;
        var next: Nat;
    };

    public func init<T>(maxSize: Nat): SparseQueue<T> {
        {
            var tree = BTree.init(null);
            var order = BTree.init(null);
            maxSize;
            var next = 0;
        }
    };

    public func clearOld<T>(queue: SparseQueue<T>, before: Time.Time) {
        loop {
            let ?(time, guid) = BTree.entries(queue.order).next() else {
                return;
            };
            if (time < before) {
                ignore BTree.delete(queue.order, Int.compare, time);
                ignore BTree.delete(queue.tree, Blob.compare, guid);
            } else {
                return;
            };
        };
    };

    /// It returns `value` or an old value. TODO: It is error prone.
    public func add<T>(queue: SparseQueue<T>, guid: GUID, value: T): T {
        switch (BTree.get(queue.tree, Blob.compare, guid)) {
            case (?(_, v)) return v; // already there is
            case (null) {};
        };
        if (BTree.size(queue.order) >= queue.maxSize) {
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
        ignore BTree.insert(queue.tree, Blob.compare, guid, (k, value));
        ignore BTree.insert(queue.order, Int.compare, k, guid);
        queue.next += 1;
        value;
    };

    public func delete<T>(queue: SparseQueue<T>, key: GUID) {
        let v0 = BTree.get(queue.tree, Blob.compare, key);
        let ?(time, _) = v0 else {
            Debug.trap("programming error");
        };
        ignore BTree.delete(queue.order, Int.compare, time);
        ignore BTree.delete(queue.tree, Blob.compare, key);
    };

    public func get<T>(queue: SparseQueue<T>, key: GUID): ?T {
        do ? { BTree.get(queue.tree, Blob.compare, key)!.1 };
    };
}