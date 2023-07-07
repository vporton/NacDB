import { Worker } from 'worker_threads';
// import * as _ from 'lodash';

function runThread() {
  const worker = new Worker('./worker.js', { workerData: {} });

  worker.on('message', (response) => {
    console.log(`Response from worker:`, response);
  });

  worker.on('error', (error) => {
    console.error(`Error in worker:`, error);
  });

  worker.on('exit', (code) => {
    // if (code !== 0) {
      console.error(`Worker stopped with exit code ${code}`);
    // }
  });
}

console.log("Starting threads...")
runThread();
runThread();
