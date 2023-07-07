"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
var worker_threads_1 = require("worker_threads");
function runThread() {
    var worker = new worker_threads_1.Worker('worker.ts', { workerData: {} });
    worker.on('message', function (response) {
        console.log("Response from worker:", response);
    });
    worker.on('error', function (error) {
        console.error("Error in worker:", error);
    });
    worker.on('exit', function (code) {
        // if (code !== 0) {
        console.error("Worker stopped with exit code ".concat(code));
        // }
    });
}
console.log("Starting threads...");
runThread();
runThread();
