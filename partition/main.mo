shared({caller}) actor class Partition() = this {
    public composite query func scanLimitOuterComposite()
        : async ()
    {
        await* N.scanLimitOuter();
    };

    module N {
        type Test = actor {
            scanLimitInner: query() -> async ();
        };

        /// Retrieve sub-DB entries by its outer key.
        public func scanLimitOuter(): async* () {
            let part: Test = actor("aaaaa-aa");
            await part.scanLimitInner();
        };
    };
}