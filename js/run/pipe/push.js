const HTTPPipe = require('../../pipe/http');

const go = async () => {
  let count = parseInt(process.env.COUNT);
  if (!count) {
    count = 0;
  }
  let threads = 1;
  if (process.env.THREADS) {
    threads = parseInt(process.env.THREADS);
  }
  if (threads === NaN || threads < 1) {
    threads = 1;
  }
  const url = process.env.PIPELINR_URL;
  const apikey = process.env.PIPELINR_API_KEY;

  sender = new HTTPPipe(apikey, 'load-1', url);

  for(let i = 0; i < count || count === 0; i++) {
    await sender.send({
						Route:   ["load-1", "load-2", "load-3", "load-4", "load-5"],
						Payload: `{"index":${i}}`,
    });
  }
};

go();