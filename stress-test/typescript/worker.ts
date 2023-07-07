const { workerData } = require('worker_threads');

async function run(data: {}) {
    console.log('THREAD')
}

run(workerData);
