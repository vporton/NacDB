# Statistical pseudo-random number generators for Motoko

## Overview

The package provides multiple pseudo-random number generators.

Note: The PRNGs generate _statistical_ pseudo-random numbers. They are not cryptographically secure.

Currently implemented generators:
* [Seiran128](https://github.com/andanteyk/prng-seiran)
* [SFC64](https://numpy.org/doc/stable/reference/random/bit_generators/sfc64.html), SFC32

### Links

The package is published on [Mops](https://mops.one/prng) and [GitHub](https://github.com/research-ag/prng).
Please refer to the README on GitHub where it renders properly with formulas and tables.

API documentation: [here on Mops](https://mops.one/prng/docs/lib)

For updates, help, questions, feedback and other requests related to this package join us on:

* [OpenChat group](https://oc.app/2zyqk-iqaaa-aaaar-anmra-cai)
* [Twitter](https://twitter.com/mr_research_ag)
* [Dfinity forum](https://forum.dfinity.org/)

## Usage

### Install with mops

You need `mops` installed. In your project directory run:
```
mops add prng
```

In the Motoko source file import the package as one of:
```
import Prng "mo:prng";
```

### Example

```
import Prng "mo:prng";

let seed : Nat64 = 0;

let rng = Prng.Seiran128();
rng.init(seed);
rng.next();
rng.next();

let rng2 = Prng.SFCa(); // SFCa is compatible to numpy
rng2.init(seed);
rng.next();
rng.next();
```

### Build & test

You need `moc` and `wasmtime` installed.
Then run:
```
git clone git@github.com:research-ag/prng.git
make -C test
```

## Benchmarks

The benchmarking code can be found here: [canister-profiling](https://github.com/research-ag/canister-profiling)

### Time

Wasm instructios per invocation of `next()`.

|method|Seiran128|SFC64|SFC32|
|---|---|---|---|
|next|251|377|253|

### Memory

Heap allocation per invocation of `next()`.
 
|method|Seiran128|SFC64|SFC32|
|---|---|---|---|
|next|36|48|8|

## Copyright

MR Research AG, 2023
## Authors

Main author: react0r-com

Contributors: Timo Hanke (timohanke) 
## License 

Apache-2.0
