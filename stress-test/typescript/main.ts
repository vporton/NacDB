import { Worker } from 'worker_threads';
// import * as _ from 'lodash';
import { DBOptions, MoveCap } from "../../example/src/declarations/index/index.did.js";
import { idlFactory } from '../../example/src/declarations/index';

const dbOptions: DBOptions = {
  moveCap: {'usedMemory': BigInt(500_000)},
  hardCap: [BigInt(1000)],
  partitionCycles: BigInt(300_000_000_000),
  constructor: async opts => await idlFactory(opts),
};
const indexCanister = Index(dbOptions);

function runThread() {

  const worker = new Worker('./worker.js', { workerData: {} });

  worker.on('message', (response) => {
    console.log(`Response from worker:`, response);
  });

  worker.on('error', (error) => {
    console.error(`Error in worker:`, error);
  });

  worker.on('exit', (code) => {
    if (code !== 0) {
      console.error(`Worker stopped with exit code ${code}`);
    }
  });
}

console.log("Starting threads...")
runThread();
runThread();
