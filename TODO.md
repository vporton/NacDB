TODO:

- Do not save dbOptions in a stable variable, return it from a function instead.
  Even better, save it in separate variables for each field.

- Decrease amount of cycles for partition canisters?

- It can be abused by calling with the same GUID after complete accomplishing
  of an operation.

- Documentation `mo-doc` and `typedoc`.

- Maybe, `Text` keys instead of `Nat` for `SubDBKey` (and pass this key when creating sub-DB)?