## TODO

- Stress testing is done all-wrong, because the next operation on the `referenceTree` or `resultingTree` is taken randomly from several
  threads of execution.
  The right way to test this is to pass `referenceTree` operations as `() -> ()` additional arguments to shared methods,
  for them to be executed inside "mutex"-like guards.

- Ensure that all arguments of shared functions with GUID are stored
  in the map from GUID, to ensure that a hacker cannot cause ill-effects
  by changing a part of the arguments in a repeated call.

- Retrieve the queue.

- Almost certainly we have too many locks, causing performance degradatiom.

- DBOptions should pass as a separate argument, not as a part of `DBOptions` or `SuperDB`.

- Decrease amount of cycles for partition canisters?

- Documentation `mo-doc` and `typedoc`.

- Maybe, `Text` keys instead of `Nat` for `SubDBKey` (and pass this key when creating sub-DB)?

- Make not only `stress-test` but also a regular quick test to work again.
