const retry = require('../lib/retry');
const sleep = require('../lib/sleep');
const HTTPPipe = require('../pipe/http');

class Worker {
  constructor(pipe, step) {
    this._pipe = pipe;
    this._step = step;
    this._running = false;
    this._onMessage = [];
    this._onError = [];

    this._pipe.setReceiveOptions({
      autoAck: false,
      block: false,
      count: 10,
      timeout: 60,
      redeliveryTimeout: 120,
    });
  }

  get pipe() {
    return this._pipe
  }

  set receiveOptions(receiveOptions) {
    this._pipe.setReceiveOptions(receiveOptions);
  }

  onMessage(fn) {
    this._onMessage.push(fn);
  }

  onError(fn) {
    this._onError.push(fn);
  }

  stop() {
    this._running = false;
    this.pipe.stop();
  }

  async run() {
    if (this._running) {
      throw new Error('already running');
    }

    this._running = true;

    this._pipe.start()
    await sleep(1000);

    let msg;
    while(msg = await this._pipe.next()) {
      let shouldcomplete = true;
      let keepon = true;
      for (let i = 0; i < this._onMessage.length; i++) {
        const fn = this._onMessage[i];
        if (!keepon) {
          continue;
        }

        try {
          await fn(msg);
        } catch (er) {
          // console.log('threw an error on callback', er)
          keepon = false;
          await retry( async () => {
            await this._pipe.log(msg.id.value, -1, `failed to complete handler ${i}, with error ${er}`);
          }, 40, 250);
          
          if (this._onError.length > 0) {
            shouldcomplete = false
          }
          for (let j = 0; j < this._onError.length; j++){
            await this._onError[j](m, er);
          }
        }
      }
      if (shouldcomplete) {
        await retry( async () => {
          await this._pipe.log(msg.id.value, 0, `completed step ${this._step}`);
        }, 40, 250);
        await retry( async () => {
          await this._pipe.complete(msg.id.value);  
        }, 40, 250);
      }
    }
  }
}

const NewHTTPWorker = (url, apikey, step) => {
  const pipe = new HTTPPipe(apikey, step, url);
  return new Worker(pipe, step);
}

module.exports = {
  Worker,
  NewHTTPWorker,
}