/// The intended use is a queue of operations on a canister.
/// `maxSize` protects against memory overflow.
///
/// FIXME: Delete only answered queries (to avoid half-done ops).

import Debug "mo:base/Debug";
import Nat "mo:base/Nat";
import Blob "mo:base/Blob";
import Bool "mo:base/Bool";
import Time "mo:base/Time";
import Iter "mo:base/Iter";
import Int "mo:base/Int";
import BTree "mo:stableheapbtreemap/BTree";

module {
    /// Globally unique identifier.
    public type GUID = Blob;

    /// A queue of operations of a certain kind (with an argument type `T` and result type `R`)
    /// on a canister.
    ///
    /// Treat this as an opaque type.
    public type OpsQueue<T, R> = {
        results: BTree.BTree<GUID, R>;
        unanswered: BTree.BTree<GUID, T>;
        /// Order of results
        order: BTree.BTree<Time.Time, BTree.BTree<GUID, ()>>; // TODO: or RBTree here?
        /// Max size of results
        maxSize: Nat;
        // TODO: Also introduce `maxTime`.
    };

    /// Create a queue of operations of the given max size.
    public func init<T, R>(maxSize: Nat): OpsQueue<T, R> {
        {
            results = BTree.init(null);
            unanswered = BTree.init(null);
            order = BTree.init(null);
            maxSize;
        }
    };

    /// Shorten the tree, if reached `maxSize`.
    func cutTree<T, R>(queue: OpsQueue<T, R>) {
        if (BTree.size(queue.results) == queue.maxSize) {
            let iter = BTree.entries(queue.order);
            let ?(time, subtree) = iter.next() else {
                Debug.trap("programming error");
            };
            let subiter1 = BTree.entries(subtree);
            let ?(subkey1, _) = subiter1.next() else {
                Debug.trap("programming error");
            };
            let subitem2 = subiter1.next();
            switch (subitem2) {
                case (?_) {
                    ignore BTree.delete(subtree, Blob.compare, subkey1);
                };
                case null { // just one element in the list
                    ignore BTree.delete(queue.order, Int.compare, time);
                };
            };
            ignore BTree.delete(queue.results, Blob.compare, subkey1);
        };
    };

    /// Add an operation accepting value `T` to the queue.
    /// Each operation is identified by a unique GUID `guid`. (Do not use the same GUID for two different operations.)
    public func add<T, R>(queue: OpsQueue<T, R>, guid: GUID, value: T) {
        cutTree(queue);
        if (BTree.has(queue.unanswered, Blob.compare, guid) or BTree.has(queue.results, Blob.compare, guid)) {
            Debug.print("queue already contains guid");
            Debug.trap("queue already contains guid");
        };
        let time = Time.now();
        ignore BTree.insert(queue.unanswered, Blob.compare, guid, value);
        let subtree = switch (BTree.get(queue.order, Int.compare, time)) {
            case (?subtree) { subtree };
            case null {
                let subtree = BTree.init<GUID, ()>(null);
                ignore BTree.insert(queue.order, Int.compare, time, subtree);
                subtree;
            };
        };
        ignore BTree.insert(subtree, Blob.compare, guid, ());
    };

    /// Call this function to answer a queue element operation `guid` by the return value `value`.
    public func answer<T, R>(queue: OpsQueue<T, R>, guid: GUID, value: R) {
        let v = BTree.delete(queue.unanswered, Blob.compare, guid);
        let ?_ = v else {
            Debug.trap("no such GUID")
        };
        ignore BTree.insert(queue.results, Blob.compare, guid, value);
    };

    /// Obtain the result of an operation `guid` (or return `null` if still none).
    /// This should be called no more than once per `guid`. // FIXME: Lift this restriction.
    /// FIXME: Return `R` instead of `?R`?
    public func result<T, R>(queue: OpsQueue<T, R>, guid: GUID): ?R {
        switch (BTree.get(queue.results, Blob.compare, guid)) {
            case (?result) {
                ?result;
            };
            case null {
                Debug.trap("GUID not found");
            };
        };
    };

    /// Get the argument (`T` or null if none) of an operation with GUID `key`.
    public func get<T, R>(queue: OpsQueue<T, R>, key: GUID): ?T {
        BTree.get(queue.unanswered, Blob.compare, key);
    };

    /// Is there an operation with GUID `key` in the current queue?
    public func has<T, R>(queue: OpsQueue<T, R>, key: GUID): Bool {
        BTree.has(queue.unanswered, Blob.compare, key);
    };

    /// Iterate through the queue.
    public func iter<T, R>(queue: OpsQueue<T, R>): Iter.Iter<(GUID, T)> {
        BTree.entries(queue.unanswered);
    };

    /// Execute every operation in the queue.
    ///
    /// FIXME: Require `f` to be idempotent.
    public func whilePending<T, R>(queue: OpsQueue<T, R>, f: (GUID, T) -> async* ()): async* () {
        label l loop {
            let i = BTree.entries(queue.unanswered);
            let elt = i.next();
            switch (elt) {
                case (?(guid, _)) {
                    let ?argument = BTree.get(queue.unanswered, Blob.compare, guid) else {
                        Debug.trap("OpsQueue: programming error");
                    };
                    await* f(guid, argument);
                    ignore BTree.delete(queue.unanswered, Blob.compare, guid);
                };
                case null {
                    break l;
                };
            };
        };

    };
}