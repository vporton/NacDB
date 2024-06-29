actor Partition {
    public composite query func scanLimitOuterComposite() : async ()
    {
        await* N.outer();
    };

    module N {
        public type Test = actor {
            inner: query() -> async ();
        };

        public func outer(): async* () {
            let part: Test = actor("aaaaa-aa");
            await part.inner();
        };
    };
}