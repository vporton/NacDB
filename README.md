# NacDB

This is NacDB distributed database.

The current stage of development is an not enough tested MVP.

## Architecture

NacDB is a no-SQL multicanister database. In each canister there are several sub-DBs.

Each sub-DB is seamlessly enumerable (unlike [CanDB](https://github.com/ORIGYN-SA/CanDB)).

When databases in a canister become too big or too many, a new canister is created and
a sub-database is moved to it (or, if the last canister is not yet filled, the sub-DB is
just moved to it). When a sub-DB is moved, a shared callback is called in
order for your project that may use this library to update its references to the sub-DB.
Such the architecture is chosen because of high cost of creating a new canister.

## Usage

You are recommended to copy (and possibly modify) code from
`example/src/index/` and `example/src/partition/` to use this system.
These folders contain source for the "index" (controller) canister and for
"partition" (part of the actual DB) canisters. You create only index canister
(as exampled in `example/src/example_backend`), the partition canisters will
be create by index canister automatically.

The below text in this `README` file may be inexact.

## Running the project locally

If you want to test your project locally, you can use the following commands:

```bash
# Starts the replica, running in the background
dfx start --background

# Deploys your canisters to the replica and generates your candid interface
dfx deploy
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

### Note on frontend environment variables

If you are hosting frontend code somewhere without using DFX, you may need to make one of the following adjustments to ensure your project does not fetch the root key in production:

- set`DFX_NETWORK` to `ic` if you are using Webpack
- use your own preferred method to replace `process.env.DFX_NETWORK` in the autogenerated declarations
  - Setting `canisters -> {asset_canister_id} -> declarations -> env_override to a string` in `dfx.json` will replace `process.env.DFX_NETWORK` with the string in the autogenerated declarations
- Write your own `createActor` constructor
