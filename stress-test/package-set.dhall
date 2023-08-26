let upstream = https://github.com/dfinity/vessel-package-set/releases/download/mo-0.9.1-20230516/package-set.dhall sha256:1ec31bbdea0234767f35941608d4c763b9dd9951858158057fa92a4a71b574d6
let Package =
    { name : Text, version : Text, repo : Text, dependencies : List Text }

let
  -- This is where you can add your own packages to the package-set
  additions =
    [
      { name = "stable-buffer"
      , repo = "https://github.com/canscale/StableBuffer"
      , version = "v0.2.0"
      , dependencies = [ "base" ]
      },
      { name = "btree"
      , repo = "https://github.com/canscale/StableHeapBTreeMap"
      , version = "v0.3.1"
      , dependencies = [ "base" ]
      },
      { name = "stable-rbtree"
      , repo = "https://github.com/canscale/StableRBTree"
      , version = "v0.6.1"
      , dependencies = [ "base" ]
      },
      { name = "candb" -- TODO: Remove.
      , repo = "https://github.com/ORIGYN-SA/CanDB"
      , version = "beta"
      , dependencies = [ "base" ]
      },
      { name = "prng"
      , repo = "https://github.com/research-ag/prng"
      , version = "main"
      , dependencies = [ "base" ]
      },
      { name = "encoding"
      , version = "main"
      , repo = "https://github.com/aviate-labs/encoding.mo"
      , dependencies = [ "base" ]
      },
      { name = "base-0.7.3"
      , version = "master"
      , repo = "https://github.com/dfinity/motoko-base"
      , dependencies = [] : List Text
      },
      { name = "sha"
      , version = "master"
      , repo = "https://github.com/enzoh/motoko-sha"
      , dependencies = [ "base" ]
      },
    ] : List Package

let
  {- This is where you can override existing packages in the package-set

     For example, if you wanted to use version `v2.0.0` of the foo library:
     let overrides = [
         { name = "foo"
         , version = "v2.0.0"
         , repo = "https://github.com/bar/foo"
         , dependencies = [] : List Text
         }
     ]
  -}
  overrides =
    [
      { name = "base"
      , repo = "https://github.com/dfinity/motoko-base"
      , version = "moc-0.9.3"
      , dependencies = [] : List Text
      },
      { name = "base-0.7.3"
      , repo = "https://github.com/dfinity/motoko-base"
      , version = "moc-0.9.3"
      , dependencies = [] : List Text
      },
    ] : List Package

in  upstream # additions # overrides
