const sleep = require('../../lib/sleep');
const HTTPPipe = require('../../pipe/http');

const dopipe = async (index) => {
	const step = process.env.STEP
  const url = process.env.PIPELINR_URL;
  const apikey = process.env.PIPELINR_API_KEY;

  const receiver = new HTTPPipe(apikey, step, url);
  receiver.setReceiveOptions({count: 10, timeout: 10, redeliveryTimeout: 300});
  receiver.start();

  for (let i = 1; i > 0; i++) {
    const m = await receiver.next();
    await receiver.log(m.id.value, i, `finished iteration ${i}`);
    await receiver.decorate(m.id.value, [{
      Key: `worker-${index}`,
      Value: `"${i}"`,
    }]);
    await receiver.complete(m.id.value);
    console.log(index, 'done with msg', m.id.value);
  }
}

const go = async () => {
  let threads = 1;
  if (process.env.THREADS) {
    threads = parseInt(process.env.THREADS);
  }
  if (threads === NaN || threads < 1) {
    threads = 1;
  }
  console.log('launching threads', threads);
  for (let i = 0; i < threads; i++) {
    dopipe(i);
  }

  while (true) {
    await sleep(10000);
  }
};

go();