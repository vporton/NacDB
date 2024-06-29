# StableRBTree

Stable Red Black Trees in Motoko. 

## Motivation
Inspiration taken from [this back and forth in the Dfinity developer forums](https://forum.dfinity.org/t/clarification-on-stable-types-with-examples/11075).

## API Documentation

API documentation for this library can be found at https://canscale.github.io/StableRBTree

## StableRBTree
  This module is a direct deconstruction of the object oriented [RBTree.mo class in motoko-base]
  (https://github.com/dfinity/motoko-base/blob/master/src/RBTree.mo)
  into a series of functions and is meant to be persistent across updates, with the tradeoff 
  being larger function signatures.

## Usage
Install vessel and ensure this is included in your package-set.dhall and vessel.dhall
```
import RBT "mo:stableRBT/StableRBTree";
...

// immutable updates
let t = RBT.init<Text, Nat>();
let nt = RBT.put(t, Text.compare, "John", 52);

// or mutable updates
var t = RBT.init<Text, Nat>();
t := RBT.put(t, Text.compare, "John", 52); 
```

## License
StableRBTree is distributed under the terms of the Apache License (Version 2.0).

See LICENSE for details.
