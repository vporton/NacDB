module {
    public let dbOptions = {
        moveCap = #usedMemory 500_000;
        hardCap = ?1000;
        partitionCycles = 28_000_000_000;
        timeout = 20_000_000_000; // 20 sec
        createDBQueueLength = 60;
        insertQueueLength = 10;
    };
}