module {
    public let dbOptions = {
        moveCap = #usedMemory 500_000;
        hardCap = ?1000;
        partitionCycles = 28_000_000_000;
        createDBQueueLength = 60;
        insertQueueLength = 10;
    };
}