const retry = require("../lib/retry");
const { NewHTTPPipe, NewGRPCPipe } = require("../pipe");

class Worker {
  #pipe;
  #onMessage;
  #onError;
  #running;

  constructor(pipe) {
    this.#pipe = pipe;
    this.receiveOptions = {
      autoAck: false,
      block: false,
      count: 10,
      timeout: 60,
      redeliveryTimeout: 120
    }
    this.#running = false;
    this.#onError = [];
    this.#onMessage = [];
  }

  set receiveOptions(ro) {
    this.#pipe.receiveOptions = ro;
  }

  get pipe() {
    return this.#pipe;
  }

  get name() {
    return this.#pipe.name;
  }
  get step() {
    return this.name;
  }

  // onMessage takes an async callback that takes parameters (event, pipe), where pipe is the originator of the event
  //  if the callback throws an error, then the onError chain is called
  //  if there is no onError chain, then the message is completed automatically
  //  if there is an onError chain, then the message is *not* completed nor acknowledged automatically
  onMessage(fn) {
    this.#onMessage.push(fn);
  }

  // onError takes a callback that takes parameters (error, event, pipe), where pipe is the originator of the event
  //  if the message should be completed or acknowledged, then the onError callback should do taht work
  onError(fn) {
    this.#onError.push(fn);
  }

  stop() {
    this.#running = false;
    this.#pipe.stop();
  }

  // run starts a blocking loop that will pull messages from the pipe
  //  on each message in the pipe, it will run the onMessage chain
  //  if any of the onMessage callbacks fails, the onMessage chain will stop and the onError chain will start
  //  if there is no onError chain, then the message will be completed and the runner will continue
  async run() {
    if (this.#running) {
      throw new Error('already running');
    }
    this.#running = true;
    this.#pipe.start();

    for(let msg = await this.#pipe.next(); this.#running ; msg = await this.#pipe.next()) {
      let shouldcomplete = true;
      let keepon = true;
      
      for(let i = 0; i < this.#onMessage.length; i++) {
        const fn = this.#onMessage[i];
        if(!keepon) {
          continue;
        }

        try {
          await fn(msg, this.#pipe);
        } catch(er) {
          keepon = false;
          await retry(async () => {
            await this.#pipe.log(msg.id, -1, `failed to run handler ${i}, with error ${er}`);
          }, 40, 250);

          if(this.#onError.length > 0) {
            shouldcomplete = false;
          }
          for(let j = 0; j < this.#onError.length; j++) {
            await this.#onError[j](er, msg, this.#pipe);
          }
        }
      }
      if(shouldcomplete) {
        await retry(async () => {
          await this.#pipe.log(msg.id, 0, `completed step ${this.step}`);
        }, 40, 250);
        await retry(async () => {
          await this.#pipe.complete(msg.id);
        }, 40, 250);
      }
    }
  }
}

const NewHTTPWorker = (step, apikey, url = 'https://pipelinr.dev') => {
  return new Worker(NewHTTPPipe(step, apikey, url));
}

const NewGRPCWorker = (step, apikey, url = 'grpc.pipelinr.dev:80') => {
  return new Worker(NewGRPCPipe(step, apikey, url));
}

module.exports = {
  Worker,
  NewHTTPWorker,
  NewGRPCWorker,
}