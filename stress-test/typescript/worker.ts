const { workerData } = require('worker_threads');

class Runner {
    referenceData: Map<{part: string, key: number}, Map<string, string>>;
    constructor(data: {}) {
        this.referenceData = new Map();
    }
    async run() {
        console.log("Running");
    }
}

const runner = new Runner(workerData);
runner.run()
