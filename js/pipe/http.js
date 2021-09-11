const axios = require('axios');
const retry = require('../lib/retry');
const sleep = require('../lib/sleep');
class Pipe {
  constructor(apikey, pipe, url = "http://pipelinr.dev") {
    if (!apikey) {
      throw new Error('apikey required');
    }
    if (!pipe) {
      throw new Error('pipe required');
    }
    this._apikey = apikey;
    this._url = url;
    this._messages = [];
    this._receiveOptions = {
      autoAck: false,
      block: false,
      count: 1,
      timeout: 0,
      redeliveryTimeout: 0,
      pipe: pipe,
    };
    this._running = false;
    this._axios = axios.create({
      timeout: 60000,
      baseURL: url,
      headers: {'authorization': `api ${this._apikey}`},
    });
  }
  
  // bool AutoAck = 1;
  // bool Block = 2;
  // int32 Count = 3;
  // int64 Timeout = 4;
  // string Pipe = 5;
  // int64 RedeliveryTimeout = 6;
  setReceiveOptions({autoAck = false, block = false, count = 1, timeout = 0, redeliveryTimeout = 0}) {
    this._receiveOptions = {
      pipe: this._receiveOptions.pipe,
      autoAck,
      block,
      count,
      timeout,
      redeliveryTimeout,
    };
  }

  async send({Payload, Route = []}) {
    if (!Payload) {
      throw new Error('Payload required');
    }
    if (!Route || Route.length === 0) {
      throw new Error('Route must have at least 1 element');
    }

    const result = await retry(
      async () => {
        return await this._axios.post(`/api/2/pipes`, {Payload, Route});
      },
      10,
      250,
    );

    return result.data.text;
  }

  async ack(id) {
    return await this._axios.put(`/api/2/message/${id}/ack/${this._receiveOptions.pipe}`);
  }
  async complete(id) {
    return await this._axios.put(`/api/2/message/${id}/complete/${this._receiveOptions.pipe}`);
  }
  async log(id, code, message) {
    if(code === undefined || code === null || message === undefined || message === null) {
      throw new Error('code and message required');
    }
    return await this._axios.patch(`/api/2/message/${id}/log/${this._receiveOptions.pipe}`, {
      Code: code,
      Message: message,
    });
  }
  // steps looks like [string, string]
  async addSteps(id, steps) {
    if(!steps || steps.length === 0) {
      throw new Error('steps must be an array with length > 0');
    }
    return await this._axios.patch(`/api/2/message/${id}/route`, {
      After: this._receiveOptions.pipe,
      NewSteps: steps,
    });
  }
  // decorations looks like [{Key, Value}]
  async decorate(id, decorations) {
    if(!decorations || decorations.length === 0) {
      throw new Error('decorations must be an array with length > 0');
    }
    return await this._axios.patch(`/api/2/message/${id}/decorations`, {Decorations: decorations});
  }

  async _fetchWithBackoff() {
    const parms = {};
    if (this._receiveOptions.autoAck) {
      parms.autoAck = 'yes';
    }
    if (this._receiveOptions.block) {
      parms.block = 'yes';
    }
    if (this._receiveOptions.count) {
      parms.count = this._receiveOptions.count;
    }
    if (this._receiveOptions.timeout) {
      parms.timeout = this._receiveOptions.timeout;
    } else {
      parms.timeout = 60;
    }
    if (this._receiveOptions.redeliveryTimeout) {
      parms.redeliveryTimeout = this._receiveOptions.redeliveryTimeout;
    }

    const result = await retry(
      async () => {
        const url = `/api/2/pipe/${this._receiveOptions.pipe}`;
        const res = await this._axios.get(url, {params: parms});
        return res.data.Events;
      },
      40,
      250,
    );

    return result;
  }

  async stop() {
    this._running = false;
  }

  async start() {
    if (this._running) {
      throw new Error('already running');
    }
    this._running = true;

    while (this._running) {
      if(this._messages.length < this._receiveOptions.count) {
        try {
          const events = await this._fetchWithBackoff();
          events.forEach((evt) => {
            this._messages.push(evt);
          });
        } catch (er){
          console.log('nothing to receive, sleeping a bit', er);
          await sleep(1000);
        }
      } else {
        await sleep(1);
      }
    }
  }

  // next returns the next message, not returning until there is one available
  async next() {
    while(true) {
      if (this._messages.length > 0) {
        return this._messages.shift();
      }
      await sleep(250);  
    }
  }
}

module.exports = Pipe;