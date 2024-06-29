/// Collection of pseudo-random number generators
///
/// The algorithms deliver deterministic statistical randomness,
/// not cryptographic randomness.
///
/// Algorithm 1: 128-bit Seiran PRNG\
/// See: https://github.com/andanteyk/prng-seiran
///
/// Algorithm 2: SFC64 and SFC32 (Chris Doty-Humphrey’s Small Fast Chaotic PRNG)\
/// See: https://numpy.org/doc/stable/reference/random/bit_generators/sfc64.html
///
/// Copyright: 2023 MR Research AG\
/// Main author: react0r-com\
/// Contributors: Timo Hanke (timohanke) 

import { range } "mo:base/Iter";

module {
  /// Constructs a Seiran128 generator.
  ///
  /// Example:
  /// ```motoko
  /// import Prng "mo:prng"; 
  /// let rng = Prng.Seiran128(); 
  /// ```  
  public class Seiran128() {

    // state
    var a : Nat64 = 0;
    var b : Nat64 = 0;

    /// Initializes the PRNG state with a particular seed.
    ///  
    /// Example:
    /// ```motoko
    /// import Prng "mo:prng"; 
    /// let rng = Prng.Seiran128(); 
    /// rng.init(0);
    /// ``` 
    public func init(seed : Nat64) {
      a := seed *% 6364136223846793005 +% 1442695040888963407;
      b := a *% 6364136223846793005 +% 1442695040888963407;
    };

    /// Returns one output and advances the PRNG's state.
    ///  
    /// Example:
    /// ```motoko
    /// let rng = Prng.Seiran128(); 
    /// rng.init(0);
    /// rng.next(); // -> 11_505_474_185_568_172_049
    /// ``` 
    public func next() : Nat64 {
      let result = (((a +% b) *% 9) <<> 29) +% a;

      let a_ = a;
      a := a ^ (b <<> 29);
      b := a_ ^ (b << 9);

      result;
    };

    // Given a bit polynomial, advances the state (see below functions)
    func jump(jumppoly : [Nat64]) {
      var t0 : Nat64 = 0;
      var t1 : Nat64 = 0;

      for (jp in jumppoly.vals()) {
        var w = jp;
        for (_ in range(0, 63)) {
          if (w & 1 == 1) {
            t0 ^= a;
            t1 ^= b;
          };

          w >>= 1;
          ignore next();
        };
      };

      a := t0;
      b := t1;
    };

    /// Advances the state 2^32 times.
    public func jump32() = jump([0x40165CBAE9CA6DEB, 0x688E6BFC19485AB1]);

    /// Advances the state 2^64 times.
    public func jump64() = jump([0xF4DF34E424CA5C56, 0x2FE2DE5C2E12F601]);

    /// Advances the state 2^96 times.
    public func jump96() = jump([0x185F4DF8B7634607, 0x95A98C7025F908B2]);
  };

  /// Constructs an SFC 64-bit generator.
  /// The recommended constructor arguments are: 24, 11, 3.
  ///
  /// Example:
  /// ```motoko
  /// import Prng "mo:prng"; 
  /// let rng = Prng.SFC64(24, 11, 3); 
  /// ```  
  /// For convenience, the function `SFC64a()` returns a generator constructed
  /// with the recommended parameter set (24, 11, 3).
  /// ```motoko
  /// import Prng "mo:prng"; 
  /// let rng = Prng.SFC64a(); 
  /// ```  
  public class SFC64(p : Nat64, q : Nat64, r : Nat64) {
    // state
    var a : Nat64 = 0;
    var b : Nat64 = 0;
    var c : Nat64 = 0;
    var d : Nat64 = 0;

    /// Initializes the PRNG state with a particular seed
    ///  
    /// Example:
    /// ```motoko
    /// import Prng "mo:prng"; 
    /// let rng = Prng.SFC64a(); 
    /// rng.init(0);
    /// ``` 
    public func init(seed : Nat64) = init3(seed, seed, seed);

    /// Initializes the PRNG state with a hardcoded seed.
    /// No argument is required.
    ///  
    /// Example:
    /// ```motoko
    /// import Prng "mo:prng"; 
    /// let rng = Prng.SFC64a(); 
    /// rng.init_pre();
    /// ``` 
    public func init_pre() = init(0xcafef00dbeef5eed);

    /// Initializes the PRNG state with three state variables
    ///  
    /// Example:
    /// ```motoko
    /// import Prng "mo:prng"; 
    /// let rng = Prng.SFC64a(); 
    /// rng.init3(0, 1, 2);
    /// ``` 
    public func init3(seed1 : Nat64, seed2 : Nat64, seed3 : Nat64) {
      a := seed1;
      b := seed2;
      c := seed3;
      d := 1;

      for (_ in range(0, 11)) ignore next();
    };

    /// Returns one output and advances the PRNG's state
    ///  
    /// Example:
    /// ```motoko
    /// let rng = Prng.SFC64a(); 
    /// rng.init(0);
    /// rng.next(); // -> 4_237_781_876_154_851_393 
    /// ``` 
    public func next() : Nat64 {
      let tmp = a +% b +% d;
      a := b ^ (b >> q);
      b := c +% (c << r);
      c := (c <<> p) +% tmp;
      d +%= 1;
      tmp;
    };
  };

  /// Constructs an SFC 32-bit generator.
  /// The recommended constructor arguments are:
  ///  a) 21, 9, 3 or
  ///  b) 15, 8, 3 
  ///
  /// Example:
  /// ```motoko
  /// import Prng "mo:prng"; 
  /// let rng = Prng.SFC32(21, 9, 3); 
  /// ```  
  /// For convenience, the functions `SFC32a()` and `SFC32b()` return
  /// generators with the parameter sets a) and b) given above.
  /// ```motoko
  /// import Prng "mo:prng"; 
  /// let rng = Prng.SFC32a(); 
  /// ```  
  public class SFC32(p : Nat32, q : Nat32, r : Nat32) {
    var a : Nat32 = 0;
    var b : Nat32 = 0;
    var c : Nat32 = 0;
    var d : Nat32 = 0;

    /// Initializes the PRNG state with a particular seed
    ///  
    /// Example:
    /// ```motoko
    /// import Prng "mo:prng"; 
    /// let rng = Prng.SFC32(); 
    /// rng.init(0);
    /// ``` 
    public func init(seed : Nat32) = init3(seed, seed, seed);

    /// Initializes the PRNG state with a hardcoded seed.
    /// No argument is required.
    ///  
    /// Example:
    /// ```motoko
    /// import Prng "mo:prng"; 
    /// let rng = Prng.SFC32a(); 
    /// rng.init_pre();
    /// ``` 
    public func init_pre() = init(0xbeef5eed);

    /// Initializes the PRNG state with three seeds
    ///  
    /// Example:
    /// ```motoko
    /// import Prng "mo:prng"; 
    /// let rng = Prng.SFC32a(); 
    /// rng.init3(0, 1, 2);
    /// ``` 
    public func init3(seed1 : Nat32, seed2 : Nat32, seed3 : Nat32) {
      a := seed1;
      b := seed2;
      c := seed3;
      d := 1;

      for (_ in range(0, 11)) ignore next();
    };

    /// Returns one output and advances the PRNG's state
    ///  
    /// Example:
    /// ```motoko
    /// let rng = Prng.SFC32a(); 
    /// rng.init(0);
    /// rng.next(); // -> 1_363_572_419 
    /// ``` 
    public func next() : Nat32 {
      let tmp = a +% b +% d;
      a := b ^ (b >> q);
      b := c +% (c << r);
      c := (c <<> p) +% tmp;
      d +%= 1;
      tmp;
    };
  };

  /// SFC64a is the same as numpy.
  /// See: [sfc64_next()](https:///github.com/numpy/numpy/blob/b6d372c25fab5033b828dd9de551eb0b7fa55800/numpy/random/src/sfc64/sfc64.h#L28)
  public func SFC64a() : SFC64 { SFC64(24, 11, 3) };

  /// Ok to use
  public func SFC32a() : SFC32 { SFC32(21, 9, 3) };
  
  /// Ok to use
  public func SFC32b() : SFC32 { SFC32(15, 8, 3) };

  /// Not recommended. Use `SFC64a` version.
  public func SFC64b() : SFC64 { SFC64(25, 12, 3) };

  /// Not recommended. Use `SFC32a` or `SFC32b` version.
  public func SFC32c() : SFC32 { SFC32(25, 8, 3) };
};
