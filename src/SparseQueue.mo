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

    // TODO: Rename.
    public type SparseQueue<T> = {
        var tree: BTree.BTree<GUID, T>;
        maxSize: Nat;
    };

    public func init<T>(maxSize: Nat, timeout: Time.Time): SparseQueue<T> {
        {
            var tree = BTree.init(null);
            maxSize;
        }
    };

    public func add<T>(queue: SparseQueue<T>, guid: GUID, value: T) {
        if (BTree.size(queue.tree) == queue.maxSize) {
            Debug.trap("queue is full");
            return;
        };
        ignore BTree.insert(queue.tree, Blob.compare, guid, value);
    };

    public func delete<T>(queue: SparseQueue<T>, guid: GUID) {
        ignore BTree.delete(queue.tree, Blob.compare, guid);
    };

    public func get<T>(queue: SparseQueue<T>, key: GUID): ?T {
        BTree.get(queue.tree, Blob.compare, key);
    };

    public func has<T>(queue: SparseQueue<T>, key: GUID): Bool {
        BTree.has(queue.tree, Blob.compare, key);
    };
}