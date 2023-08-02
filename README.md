# NacDB

This is NacDB distributed database:
A database with seamless enumeration/scanning of items,
because it is split into multiple sub-DBs, each fitting in a canister.

It is anticipated that NacDB will often be used together with CanDB.

TODO:
- review code, based on review simplify it and make it more secure
- review and as necessary modify the API, then freeze the API
- do a more elaborate stress testing, with higher probability of operations that repeat recent IDs usage often

The current stage of development is a MVP. API is not yet frozen, incompatible changes are possible.

It is usually recommended to use NacDB together with CanDB, because NacDB is strong
in one specific point: seamless enumeration of its sub-databases. For example, in
a usual workflow, NacDB could store CanDB keys rather than full values.
Both CanDB and NacDB are implemented in Motoko.

TypeScript client is not provided, because it is automatically created from Candid.

## Important information

Some functions are engineered in such a way that interrupting them in the middle
(what may happen due to technical limitations of Internet Computer) leads to memory
leaks. It is an intentional engineering decision to ignore this problem, because
such cases are rate and thus the amount of leaked memory is expected not to be very big.

## Architecture: General

NacDB is a no-SQL multicanister database. In each canister there are several sub-DBs.

Each sub-DB is seamlessly enumerable (unlike [CanDB](https://github.com/ORIGYN-SA/CanDB)).

When databases in a canister become too big or too many, a new canister is created and
a sub-database is moved to it (or, if the last canister is not yet filled, the sub-DB is
just moved to it). Such the architecture is chosen because of high cost of creating a new canister.
Each sub-database has so called "outer ID" or "outer key" that does not change when
a sub-DB is moved.

When to move a sub-DB is decided by `moveCap` value of the following type, that restricts
the memory used by the canister (the move occurs when we have the actual value above `moveCap` threshold):

```motoko
type MoveCap = { #usedMemory: Nat };
```

Some functions take the argument `guid` of type `GUID = Blob`. For the database work
correctly, you pass a GUID generated by you (You are recommended to use a cryptographically secure random generator
initialized by an [entropy value](https://internetcomputer.org/docs/current/motoko/main/base/Random)) into this argument.
If an operation such as `insert` fails, you can retry it by calling again with the same `guid` argument value.
Separate operations should always have different GUIDs, for one operation not to overwrite
another.

## Architecture: Details

You are recommended to copy (and possibly modify, e.g. add access control) code from
`example/src/index/` and `example/src/partition/` to use this system.
These folders contain source for the "index" (controller) canister and for
"partition" (part of the actual DB) canisters. You create only index canister
(as exampled in `example/src/example_backend`), the partition canisters will
be created by index canister automatically.

As you see in `example/src/partition/`, each partition contains a stable variable
of type `SuperDB`. `SuperDB` contains several values of type `SubDB` (that is several
sub-databases).

As you see in `example/src/index/`, the index canister contains a stable variable of
type `DBIndex` (the common data for the entire multi-canister database).

As examples `example/src/index/` and `example/src/partition/` show, you define
shared functions using operations provided by this library on variables of types
`DBIndex` and `SuperDB`.

Keys in `SuperDB` (identifying sub-databases) are of the type `SubDBKey = Nat`.
keys in sub-DBs are of the type `SK = Text`. Values stored in the sub-DBs are
of type `AttributeKey` defined similarly to the same-named type in CanDB, but
I chose to return `AttributeKey` directly, not a map of values (as in CanDB).

## Looser Items

Each sub-DB has optional `Nat` value `hardCap`. If the number of items in the sub-DB
reaches this number, the value with the smallest (`SK = Text`) key is removed (it is
useful among other things to ensure that a sub-DB fits into a canister).

## More on examples

`example/src/example_backend` shows an example of usage of the system:

The example uses `insert` and `get` functions (as defined in `example/src/partition/`)
to store and again retrieve a value to/from a sub-DB. There are also `has` (for an element
of a sub-DB), `hasSubDB`, `delete` (for an element of a sub-DB), `deleteSubDB`, `subDBSize`,
`createSubDB`, and for enumeration of elements of a sub-DB `iter`, `entries`, and `entriesRev`
(the reverse order iterator), as well as `scanLimit` that returns an array instead of an
iterator.

## Locking

In `src/partition/` there is used locking by boolean flags: `?...` variable `moving`
and `Bool` variable `moving`. While these flags are set, both write and read operations
fail (and need to be repeated).

## Testing the project locally

If you want to test your project locally, you can use the following commands inside
the folder `example/`:

```bash
# Starts the replica, running in the background
dfx start --background

# Deploys your canisters to the replica and generates your candid interface
make deploy
```

Once the job completes, your application will be available at `http://localhost:4943?canisterId={asset_canister_id}`.

If you have made changes to your backend canister, you can generate a new candid interface with

```bash
npm run generate
```

at any time. This is recommended before starting the frontend development server, and will be run automatically any time you run `dfx deploy`.

If you are making frontend changes, you can start a development server with

```bash
npm start
```

Which will start a server at `http://localhost:8080`, proxying API requests to the replica at port 4943.

It also has unit test (defunct after switching to stress test, because `moc`) interpreter does not support
cycle operations.

Stress test running 10 threads, each doing 1000 random operations is activated by commands

```sh
cd stress-test
make
```

### Note on frontend environment variables

If you are hosting frontend code somewhere without using DFX, you may need to make one of the following adjustments to ensure your project does not fetch the root key in production:

- set `DFX_NETWORK` to `ic` if you are using Webpack
- use your own preferred method to replace `process.env.DFX_NETWORK` in the autogenerated declarations
  - Setting `canisters -> {asset_canister_id} -> declarations -> env_override to a string` in `dfx.json` will replace `process.env.DFX_NETWORK` with the string in the autogenerated declarations
- Write your own `createActor` constructor
