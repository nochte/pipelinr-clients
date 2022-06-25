const axios = require('axios');
const event = require('../../entities/event');
const dot = require('dot-object');
const _ = require('lodash');

const retry = require("../../lib/retry");

class Driver {
  #client;

  constructor(apikey, url = "https://pipelinr.dev") {
    if (!apikey) {
      throw new Error('apikey required');
    }
    if (!url) {
      throw new Error('url required');
    }

    this.#client = axios.create({
      timeout: 60000,
      baseURL: url,
      headers: {authorization: `api ${apikey}`}
    })
  }

  async send({Payload, Route = []}) {
    if(!Payload) {
      throw new Error('Payload required');
    }
    if(!Route || Route.length === 0) {
      throw new Error('Route must have at least 1 element');
    }

    const result = await retry(
      async () => {
        return await this.#client.post(`/api/2/pipes`, {Payload, Route});
      },
      10, 250,
    );

    return result.data.text;
  }

  async recv(receiveoptions) {
    const res = await this.#client.get(`/api/2/pipe/${receiveoptions.pipe}`, {params: receiveoptions.HTTPFormat});
    return res.data.Events.map(event.fromHTTPFormat);
  }

  async ack(id, step) {
    const result = await this.#client.put(`/api/2/message/${id}/ack/${step}`);
    if (result.data.status === 200) {
      return true;
    }
    throw new Error('not found');
  }

  async complete(id, step) {
    try {
      const res = await this.#client.put(`/api/2/message/${id}/complete/${step}`);
      return true;
    } catch (er) {
      if(er.response && er.response.data && er.response.data.text === 'step cannot be completed, out of order') {
        return false;
      }
      throw er;
    }
  }

  async appendLog(id, routelog) {
    const result = await this.#client.patch(`/api/2/message/${id}/log/${routelog.step}`, routelog.HTTPFormat);
    if(result.data.status === 200) {
      return true;
    }
    throw new Error('not found');
  }

  async addStepsAfter(id, after, steps) {
    const result = await this.#client.patch(`/api/2/message/${id}/route`, {
      After: after,
      NewSteps: steps
    });
    if (result.data.status === 200) {
      return true;
    }
    throw new Error('not found');
  }

  async decorate(id, decorations) {
    const result = await this.#client.patch(`/api/2/message/${id}/decorations`, {Decorations: decorations.HTTPFormat});
    return result.data.map((r) => { return r.status === 200 });
  }

  async getDecorations(id, keys) {
    const result = await this.#client.get(`/api/2/message/${id}/decorations`, {params: { keys: keys.join(',') }});
    return _.merge(
      ...result.data.Decorations.map(({Key, Value}) => {
        return dot.object({[Key]: JSON.parse(Value)});
      })
    );
  }
}

module.exports = Driver;