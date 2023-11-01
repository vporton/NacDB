FIXME:

- Stress test sometimes does not pass.
  Moreover, it is sometimes stuck!

TODO:

- Ensure that all arguments of shared functions with GUID are stored
  in the map from GUID, to ensure that a hacker cannot cause ill-effects
  by changing a part of the arguments in a repeated call.

- Almost certainly we have too many locks, causing performance degradatiom.

- Decrease amount of cycles for partition canisters?

- It can be abused by calling with the same GUID after complete accomplishing
  of an operation.

- Documentation `mo-doc` and `typedoc`.

- Maybe, `Text` keys instead of `Nat` for `SubDBKey` (and pass this key when creating sub-DB)?