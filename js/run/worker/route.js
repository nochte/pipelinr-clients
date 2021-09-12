const _ = require('lodash');
const retry = require('../../lib/retry');
const { NewHTTPWorker } = require("../../worker/worker");

const routes = [
  "node1",
  "node2",
  "node3",
  "node4",
  "node5",
  "node6",
  "node7",
  "node8",
  "node9",
  "node10",
  "node11",
  "node12"
];

let threads = 1;
if(process.env.THREADS) {
  threads = parseInt(process.env.THREADS);
}

// stolen from https://stackoverflow.com/questions/1527803/generating-random-whole-numbers-in-javascript-in-a-specific-range
function getRandomInt(min, max) {
  min = Math.ceil(min);
  max = Math.floor(max);
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

const go = async () => {
  const worker = NewHTTPWorker(process.env.PIPELINR_URL, process.env.PIPELINR_API_KEY, 'route');
  worker.onMessage(async (msg) => {
    const newroute = _.shuffle(routes).slice(0, getRandomInt(1, routes.length));
    newroute.push('graph');
    console.log('adding to route', msg.id.value, newroute);
    await retry(async () => {
      await worker.pipe.addSteps(msg.id.value, newroute);
    }, 10, 250);
  });

  worker.run();
};

for (let i = 0; i < threads; i++) {
  go();
}
