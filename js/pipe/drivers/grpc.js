const grpc = require("@grpc/grpc-js");
const event = require('../../entities/event');
const _ = require('lodash');
const dot = require('dot-object');

const {
  services: { PipeClient },
  messages: { 
    MessageEnvelop, 
    CompleteRequest,
    RouteLogRequest,
    AddStepsRequest,
    GetDecorationRequest
  },
} = require("@pipelinr/protocol");
const retry = require("../../lib/retry");

class Driver {
  #client;
  #meta

  constructor(apikey, url = "grpc.pipelinr.dev:80") {
    if (!apikey) {
      throw new Error('apikey required');
    }
    if (!url) {
      throw new Error('url required');
    }

    this.#client = new PipeClient(url, grpc.credentials.createInsecure());
    this.#meta = new grpc.Metadata()
    this.#meta.add('authorization', apikey);
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
        return new Promise((resolve, reject) => {
          const m = new MessageEnvelop();
          m.setPayload(Payload);
          m.setRouteList(Route);

          this.#client.send(m, this.#meta, (er, res) => {
            if (er) {
              return reject(er);
            }
            return resolve(res.toObject().id);
          });
        });
      }, 10, 250
    );
    return result;
  }

  async recv(receiveoptions) {
    return new Promise((resolve, reject) => {
      this.#client.recv(receiveoptions.GRPCFormat, this.#meta, (er, res) => {
        if(er) {
          return reject(er);
        }
        return resolve(
          res.toObject().eventsList.map(event.fromGRPCFormat)
        );
      });
    });
  }

  async ack(id, step) {
    const cr = new CompleteRequest();
    cr.setId(id);
    cr.setStep(step);

    return new Promise((resolve, reject) => {
      this.#client.ack(cr, this.#meta, (er, res) => {
        if(er) {
          return reject(er);
        }
        const ob = res.toObject();
        return resolve(ob.ok);
      });
    });
  }

  async complete(id, step) {
    const cr = new CompleteRequest();
    cr.setId(id);
    cr.setStep(step);

    return new Promise((resolve, reject) => {
      this.#client.complete(cr, this.#meta, (er, res) => {
        if(er) {
          if(er.details === 'step cannot be completed, out of order') {
            return resolve(false);
          }
          return reject(er);
        }
        const ob = res.toObject();
        return resolve(ob.ok);
      });
    });
  }

  async appendLog(id, routelog) {
    const rlr = new RouteLogRequest();
    rlr.setId(id);
    rlr.setLog(routelog.GRPCFormat);

    return new Promise((resolve, reject) => {
      this.#client.appendLog(rlr, this.#meta, (er, res) => {
        if(er) {
          return reject(er);
        }
        const ob = res.toObject();
        return resolve(ob.ok);
      });
    });
  }

  async addStepsAfter(id, after, steps) {
    const asr = new AddStepsRequest();
    asr.setId(id);
    asr.setAfter(after);
    asr.setNewstepsList(steps);

    return new Promise((resolve, reject) => {
      this.#client.addSteps(asr, this.#meta, (er, res) => {
        if(er) {
          return reject(er);
        }
        const ob = res.toObject();
        return resolve(ob.ok);
      });
    });
  }

  async decorate(id, decorations) {
    const d = decorations.GRPCFormat;
    d.setId(id);

    return new Promise((resolve, reject) => {
      this.#client.decorate(d, this.#meta, (er, res) => {
        if(er) {
          return reject(er);
        }
        const obs = res.toObject();
        return resolve(obs.genericresponsesList.map((r) => { return r.ok }));
      })
    })
  }

  async getDecorations(id, keys) {
    const req = new GetDecorationRequest();
    req.setId(id);
    req.setKeysList(keys)

    return new Promise((resolve, reject) => {
      this.#client.getDecorations(req, this.#meta, (er, res) => {
        if(er) {
          return reject(er);
        }
        const ob = res.toObject();
        return resolve(
          _.merge(
            ...ob.decorationsList.map(({key, value}) => {
              return dot.object({[key]: value ? JSON.parse(value) : null});
            })
        ));
      })
    })
  }
}

module.exports = Driver;