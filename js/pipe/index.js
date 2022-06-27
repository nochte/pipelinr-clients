const _ = require('lodash');
const { Decorations } = require('../entities/decorations');
const { ReceiveOptions } = require('../entities/receiveOptions');
const { RouteLog } = require('../entities/routeLog');
const retry = require('../lib/retry');
const sleep = require('../lib/sleep');
const drivers = require('./drivers');

class Pipe {
  #driver;
  #step;
  #receiveOptions;
  #attemptCount;
  #backoffMs;
  #running = false;
  #stopped = false;
  #debugMode = false;
  #messages;

  constructor(driver, step, debugMode = false) {
    if(!driver) {
      throw new Error('driver required');
    }
    if(!step) {
      throw new Error('step required');
    }

    this.#driver = driver;
    this.#step = step;
    this.receiveOptions = {
      pipe: step,
      autoAck: false,
      block: false,
      excludeRouting: false,
      excludeRouteLog: false,
      excludeDecoratedPayload: false,
      count: 1,
      timeout: 0,
      redeliveryTimeout: 60
    };
    this.retryPolicy = {attemptCount: 10, backoffMs: 250};
    this.#debugMode = debugMode;
    this.#messages = [];
  }

  get name() {
    return this.#step;
  }

  get step() {
    return this.name;
  }

  stop() {
    this.#stopped = true;
    this.#running = false;
  }

  set receiveOptions({autoAck, block, count, timeout, redeliveryTimeout, excludeRouting, excludeRouteLog, excludeDecoratedPayload}) {
    const oldro = this.#receiveOptions;
    autoAck = _.isNil(autoAck) ? oldro.autoAck : autoAck;
    block = _.isNil(block) ? oldro.block : block;
    excludeRouting = _.isNil(excludeRouting) ? oldro.excludeRouting : excludeRouting;
    excludeRouteLog = _.isNil(excludeRouteLog) ? oldro.excludeRouteLog : excludeRouteLog;
    excludeDecoratedPayload = _.isNil(excludeDecoratedPayload) ? oldro.excludeDecoratedPayload : excludeDecoratedPayload;
    count = _.isNil(count) ? oldro.count : count;
    timeout = _.isNil(timeout) ? oldro.timeout : timeout;
    redeliveryTimeout = _.isNil(redeliveryTimeout) ? oldro.redeliveryTimeout : redeliveryTimeout;

    const opts = {autoAck, block, count, timeout, redeliveryTimeout, pipe: this.step, excludeRouting, excludeRouteLog, excludeDecoratedPayload};
    this.#receiveOptions = new ReceiveOptions(opts);
  }

  get receiveOptions() {
    // return _.cloneDeep(this.#receiveOptions);
    return this.#receiveOptions
  }

  set retryPolicy({attemptCount = 10, backoffMs = 250}) {
    this.#attemptCount = attemptCount;
    this.#backoffMs = backoffMs;
  }

  async send({Payload, Route = []}) {
    if (!Payload) {
      throw new Error('Payload required');
    }
    if (!Route || Route.length === 0) {
      throw new Error('Route must have at least 1 element');
    }

    return retry(
      async () => {
        return await this.#driver.send({Payload, Route});
      },
      this.#attemptCount,
      this.#backoffMs
    );
  }

  async ack(id) {
    return this.#driver.ack(id, this.step);
  }

  async complete(id) {
    return this.#driver.complete(id, this.step);
  }

  async log(id, code, message) {
    if(code === undefined || code === null || message === undefined || message === null) {
      throw new Error('code and message required');
    }
    return this.#driver.appendLog(id, new RouteLog({
      step: this.step,
      code,
      message
    }));
  }

  async addSteps(id, steps) {
    if(!steps || steps.length === 0) {
      throw new Error('steps must be an array with length > 0');
    }

    return this.#driver.addStepsAfter(id, this.step, steps);
  }

  async decorate(id, decorations) {
    if(!decorations || decorations.length === 0) {
      throw new Error('decorations must be an array with length > 0');
    }

    return this.#driver.decorate(id, new Decorations(decorations));
  }

  async getDecorations(id, keys) {
    if(!keys || keys.length === 0) {
      throw new Error('keys must be an array with length > 0');
    }

    return this.#driver.getDecorations(id, keys);
  }

  async #fetchWithBackoff() {
    const startTime = new Date();
    let iterations = 0;

    return retry(
      async () => {
        if(this.#stopped) {
          return [];
        }
        if(iterations && this.#debugMode) {
          console.log('trying', this.step, 'elapsed', new Date() - startTime, 'ms. iteration', iterations);
        }
        iterations++;
        return this.#driver.recv(this.#receiveOptions);
      }, 
      this.#attemptCount, 
      this.#backoffMs
    );
  }

  async fetch() {
    return this.#driver.recv(this.#receiveOptions);
  }

  // maxmessages > 0 indicates that the pipe should auto-stop
  //  after receiving maxmessages number of messages
  async start(maxmessages = 0) {
    if (this.#running) {
      throw new Error('already running');
    }
    this.#running = true;
    let processedMessages = 0;
    const startTime = new Date();

    while (this.#running) {
      if(this.#messages.length < this.#receiveOptions.count) {
        if(this.#debugMode){
          console.log(this.step, 'pipe is not full, fetching some', this.#messages.length, `(${processedMessages})`);
        }
        try {
          const events = await this.#fetchWithBackoff();
          events.forEach((evt) => {
            this.#messages.push(evt)
          });
          if(maxmessages && processedMessages >= maxmessages) {
            console.log(this.#step, 'processed through all messages in', new Date() - startTime, 'ms');
            this.stop()
          }
        } catch(er) {
          if(this.#debugMode) {
            console.log('er', er);
          }
          // console.log('nothing to receive, sleeping a bit', this.step);
          await sleep(1000);
        }
      } else {
        if(this.#debugMode) {
          console.log(this.step, 'pipe is full, sleeping a moment');
        }
        await sleep(1000);
      }
    }
  }

  async next(blocking = false) {
    while (blocking || this.#running) {
      if(this.#messages.length > 0) {
        return this.#messages.shift();
      }
      await sleep(100);
    }
  }
}

const NewHTTPPipe = (step, apikey, url = 'https://pipelinr.dev') => {
  const driver = new drivers.HTTP(apikey, url);
  return new Pipe(driver, step, process.env.DEBUG);
}

const NewGRPCPipe = (step, apikey, url = 'grpc.pipelinr.dev:80') => {
  const driver = new drivers.GRPC(apikey, url);
  return new Pipe(driver, step, process.env.DEBUG);
}

module.exports = {
  Pipe,
  NewHTTPPipe,
  NewGRPCPipe
}