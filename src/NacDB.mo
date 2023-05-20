import Principal "mo:base/Principal";

module {
    type DB = {
        pk: Principal;
        subDB: Nat32;
    };
}