import Debug "mo:base/Debug";
import Nat "mo:base/Nat";
import Blob "mo:base/Blob";
import Bool "mo:base/Bool";
import Time "mo:base/Time";
import Int "mo:base/Int";
import Iter "mo:base/Iter";
import BTree "mo:stableheapbtreemap/BTree";

/// The intended use is a queue of operations on a canister.
/// `maxSize` protects against memory overflow.
module {
    public type GUID = Blob;

    public type OpsQueue<T, R> = {
        var arguments: BTree.BTree<GUID, T>;
        var results: BTree.BTree<GUID, (R, Time.Time)>;
        var resultsOrder: BTree.BTree<Time.Time, BTree.BTree<GUID, ()>>; // or RBTree here?
        maxSize: Nat;
    };

    public func init<T, R>(maxSize: Nat): OpsQueue<T, R> {
        {
            var arguments = BTree.init(null);
            var results = BTree.init(null);
            var resultsOrder = BTree.init(null);
            maxSize;
        }
    };

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

    public func get<T, R>(queue: OpsQueue<T, R>, key: GUID): ?T {
        BTree.get(queue.arguments, Blob.compare, key);
    };

    public func has<T, R>(queue: OpsQueue<T, R>, key: GUID): Bool {
        BTree.has(queue.arguments, Blob.compare, key);
    };

    public func iter<T, R>(queue: OpsQueue<T, R>): Iter.Iter<(GUID, T)> {
        BTree.entries(queue.arguments);
    };

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