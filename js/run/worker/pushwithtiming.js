const sleep = require('../../lib/sleep');
const HTTPPipe = require('../../pipe/http');
const numpipes = 500;
let count = 0;
if (process.env.COUNT) {
  count = parseInt(process.env.COUNT);
}
let persec = 1;
if (process.env.PER_SECOND) {
  persec = parseInt(process.env.PER_SECOND);
}
const pipes = [];
for(let i = 0; i < numpipes; i++) {
  pipes.push(new HTTPPipe(process.env.PIPELINR_API_KEY, 'route', process.env.PIPELINR_URL));
}
console.log('pipes initialized');

let messagesPushed = 0;
const watchMessagesPushed = async () => {
  const st = new Date();
  while (true) {
    const elapsed = (new Date() - st)/1000;
    const mps = messagesPushed / elapsed;
    console.log(`pushed ${messagesPushed} (${mps} mps)`)
    if(count > 0 && messagesPushed >= count) {
      console.log(`${messagesPushed} pushed in total, killing process now`);
      process.exit(0);
    }
    await sleep(1000);
  }
}

const buffer = [];
const workBuffer = async () => {
  let pipendx = 0;
  while (true) {
    const msg = buffer.shift();
    if(msg === undefined) {
      await sleep(50);
      continue;
    }
    pipes[pipendx%numpipes].send(msg).then(() => {
      messagesPushed+=1;
    });
    pipendx+=1;
  }
}

const go = async () => {
  const timetosleep = 1000/persec;
  for(let i = 0; count == 0 || i < count; i++) {
    buffer.push({
      Route:   ['route'],
      Payload: `{"index":${i}}`,
    });
    await sleep(timetosleep);
  }
};

watchMessagesPushed();
workBuffer();
go();
