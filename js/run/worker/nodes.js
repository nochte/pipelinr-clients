const retry = require("../../lib/retry");
const { NewHTTPWorker } = require("../../worker/worker");

let whichnodes = process.env.NODES ? process.env.NODES.split(',') : [];
if(whichnodes.length === 0) {
  whichnodes = [
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
    "node12",
  ];
}

const launchnode = async (nodename) => {
  const worker = NewHTTPWorker(process.env.PIPELINR_URL, process.env.PIPELINR_API_KEY, nodename);
  let i = 0;
  worker.receiveOptions = {redeliveryTimeout: 300};
  worker.onMessage(async (msg) => {
    console.log(nodename, 'handling', msg.id.value)
    await retry(async () => {
      await worker.pipe.log(msg.id.value, 1, "decoration added");
    }, 10, 250);
    await retry(async () => {
      await worker.pipe.decorate(msg.id.value, [{Key: nodename, Value: `${i}`}]);
    }, 10, 250);
    i++;
  });
  worker.run();
}

const go = async () => {
  whichnodes.forEach(launchnode);
}

go();