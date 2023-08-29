import Debug "mo:base/Debug";
import Nat "mo:base/Nat";
import Blob "mo:base/Blob";
import Bool "mo:base/Bool";
import Time "mo:base/Time";
import Int "mo:base/Int";
import BTree "mo:btree/BTree";
import RBT "mo:stable-rbtree/StableRBTree";

/// The intended use is a queue of operations on a canister.
/// `maxSize` protects against memory overflow.
module {
    public type GUID = Blob;

    public type SparseQueue<T> = {
        var tree: BTree.BTree<GUID, (T, Time.Time)>;
        var order: BTree.BTree<Time.Time, RBT.Tree<GUID, ()>>;
        maxSize: Nat;
        timeout: Time.Time; // When to clear old items.
    };

    public func init<T>(maxSize: Nat, timeout: Time.Time): SparseQueue<T> {
        {
            var tree = BTree.init(null);
            var order = BTree.init(null);
            maxSize;
            timeout;
        }
    };

    private func clearOld<T>(queue: SparseQueue<T>, before: Time.Time) {
        loop {
            let ?(time, subtree) = BTree.entries(queue.order).next() else {
                return;
            };
            if (time < before) {
                var i = RBT.entries<GUID, ()>(subtree);
                label R loop {
                    let ?(guid, _) = i.next() else {
                        break R;
                    };
                    ignore BTree.delete(queue.tree, Blob.compare, guid);
                };
                ignore BTree.delete(queue.order, Int.compare, time);
            } else {
                return;
            };
        };
    };

    /// It returns `value` or an old value. TODO: It is error prone.
    public func add<T>(queue: SparseQueue<T>, guid: GUID, value: T): T {
        clearOld(queue, Time.now() - queue.timeout);

        switch (BTree.get(queue.tree, Blob.compare, guid)) {
            case (?v) return v.0; // already there is
            case (null) {};
        };
        Debug.print("QUEUE SIZE: " # debug_show(BTree.size(queue.order)));
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
        let time = Time.now();
        ignore BTree.insert(queue.tree, Blob.compare, guid, (value, time));
        let subtree = switch (BTree.get(queue.order, Int.compare, time)) {
            case (?subtree) { subtree };
            case (null) { RBT.init() };
        };
        let newSubtree = RBT.put(subtree, Blob.compare, guid, ());
        ignore BTree.insert(queue.order, Int.compare, time, newSubtree);
        value;
    };

    public func delete<T>(queue: SparseQueue<T>, key: GUID) {
        let v0 = BTree.get(queue.tree, Blob.compare, key);
        let ?(_, time) = v0 else {
            Debug.trap("programming error");
        };
        let ?subtree = BTree.get(queue.order, Int.compare, time) else {
            Debug.trap("programming error");
        };
        let subtree2 = RBT.delete(subtree, Blob.compare, key);
        if (RBT.size(subtree2) == 0) {
            ignore BTree.delete(queue.order, Int.compare, time);
        };
        ignore BTree.delete(queue.tree, Blob.compare, key);
    };

    public func get<T>(queue: SparseQueue<T>, key: GUID): ?T {
        do ? { BTree.get(queue.tree, Blob.compare, key)!.0 };
    };
}