/// The intended use is a queue of operations on a canister.
/// `maxSize` protects against memory overflow.

import Debug "mo:base/Debug";
import Nat "mo:base/Nat";
import Blob "mo:base/Blob";
import Bool "mo:base/Bool";
import Time "mo:base/Time";
import Int "mo:base/Int";
import Iter "mo:base/Iter";
import BTree "mo:stableheapbtreemap/BTree";

module {
    /// Globally unique identifier.
    public type GUID = Blob;

    /// A queue of operations of a certain kind (with an argument type `T` and result type `R`)
    /// on a canister.
    ///
    /// Treat this as an opaque type.
    public type OpsQueue<T, R> = {
        var arguments: BTree.BTree<GUID, T>;
        var results: BTree.BTree<GUID, (R, Time.Time)>;
        var resultsOrder: BTree.BTree<Time.Time, BTree.BTree<GUID, ()>>; // or RBTree here?
        maxSize: Nat;
    };

    /// Create a queue of operations of the given max size.
    public func init<T, R>(maxSize: Nat): OpsQueue<T, R> {
        {
            var arguments = BTree.init(null);
            var results = BTree.init(null);
            var resultsOrder = BTree.init(null);
            maxSize;
        }
    };

    /// Add an operation accepting value `T` to the queue.
    /// Each operation is identified by a unique GUID `guid`. (Do not use the same GUID for two different operations.)
    public func add<T, R>(queue: OpsQueue<T, R>, guid: GUID, value: T) {
        if (BTree.size(queue.arguments) == queue.maxSize) {
            Debug.trap("queue is full");
            return;
        };
        if (BTree.has(queue.arguments, Blob.compare, guid)) {
            Debug.print("queue already contains guid");
            Debug.trap("queue already contains guid");
        };
        ignore BTree.insert(queue.arguments, Blob.compare, guid, value);
    };

    /// Call this function to answer a queue element operation `guid` by the return value `value`.
    public func answer<T, R>(queue: OpsQueue<T, R>, guid: GUID, value: R) {
        ignore BTree.delete(queue.arguments, Blob.compare, guid);
        if (BTree.size(queue.resultsOrder) == queue.maxSize) {
            switch (BTree.entries(queue.resultsOrder).next()) {
                case (?(time, subtree)) {
                    switch (BTree.entries(subtree).next()) {
                        case (?(guid, ())) {
                            if (BTree.size(subtree) == 1) { // last element in subtree
                                ignore BTree.delete(queue.resultsOrder, Int.compare, time);
                            } else {
                                ignore BTree.delete(subtree, Blob.compare, guid);
                            };
                        };
                        case null {}
                    };
                };
                case null {};
            };
        };
        switch (BTree.get(queue.resultsOrder, Int.compare, Time.now())) {
            case (?subtree) {
                ignore BTree.insert(subtree, Blob.compare, guid, ());
            };
            case null {
                var subtree: BTree.BTree<GUID, ()> = BTree.init(null);
                ignore BTree.insert(subtree, Blob.compare, guid, ());
                ignore BTree.insert(queue.resultsOrder, Int.compare, Time.now(), subtree);
            };
        };
        ignore BTree.insert(queue.results, Blob.compare, guid, (value, Time.now()));
    };

    /// Obtain the result of an operation `guid` (or return `null` if still none).
    /// This should be called no more than once per `guid`.
    public func result<T, R>(queue: OpsQueue<T, R>, guid: GUID): ?R {
        switch (BTree.delete(queue.results, Blob.compare, guid)) {
            case (?(result, time)) {
               switch (BTree.get(queue.resultsOrder, Int.compare, time)) {
                    case (?subtree) {
                        if (BTree.size(subtree) == 1) { // last element in subtree
                            ignore BTree.delete(queue.resultsOrder, Int.compare, time);
                        } else {
                            ignore BTree.delete(subtree, Blob.compare, guid);
                        };
                    };
                    case null {
                        Debug.trap("OpsQueue: programming error");
                    };
                };
                ?result;
            };
            case null {
                Debug.trap("guid not found");
            };
        };
    };

    /// Get the argument (`T` or null if none) of an operation with GUID `key`.
    public func get<T, R>(queue: OpsQueue<T, R>, key: GUID): ?T {
        BTree.get(queue.arguments, Blob.compare, key);
    };

    /// Is there an operation with GUID `key` in the current queue?
    public func has<T, R>(queue: OpsQueue<T, R>, key: GUID): Bool {
        BTree.has(queue.arguments, Blob.compare, key);
    };

    /// Iterate through the queue.
    public func iter<T, R>(queue: OpsQueue<T, R>): Iter.Iter<(GUID, T)> {
        BTree.entries(queue.arguments);
    };

    /// Execute every operation in the queue.
    public func whilePending<T, R>(queue: OpsQueue<T, R>, f: (GUID, T) -> async* ()): async* () {
        let i = iter(queue);
        label l loop {
            let elt = i.next();
            switch (elt) {
                case (?(guid, elt)) {
                    await* f(guid, elt);
                };
                case null {
                    break l;
                };
            };
        };

    };
}