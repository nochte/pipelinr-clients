const _ = require('lodash');

const {
  messages
} = require("@pipelinr/protocol");

class ReceiveOptions {
  #props;
  constructor(props) {
    this.#props = _.reduce(props, (res, val, key) => {
      res[_.camelCase(key)] = val;
      return res;
    }, {});
  }

  get pipe() {
    return this.#props.pipe;
  }
  get autoAck() {
    return this.#props.autoAck;
  }
  get excludeRouting() {
    return this.#props.excludeRouting;
  }
  get excludeRouteLog() {
    return this.#props.excludeRouteLog;
  }
  get excludeDecoratedPayload() {
    return this.#props.excludeDecoratedPayload;
  }
  get block() {
    return this.#props.block;
  }
  get count() {
    return this.#props.count;
  }
  get timeout() {
    return this.#props.timeout;
  }
  get redeliveryTimeout() {
    return this.#props.redeliveryTimeout;
  }

  get GRPCFormat() {
    const recvopts = new messages.ReceiveOptions();
    recvopts.setPipe(this.pipe);
    recvopts.setAutoack(this.autoAck);
    recvopts.setBlock(this.block);
    recvopts.setCount(this.count);
    recvopts.setExcluderouting(this.excludeRouting);
    recvopts.setExcluderoutelog(this.excludeRouteLog);
    recvopts.setExcludedecoratedpayload(this.excludeDecoratedPayload);
    if(this.timeout) {
      recvopts.setTimeout(this.timeout);
    } else {
      recvopts.setTimeout(60);
    }
    if(this.redeliveryTimeout) {
      recvopts.setRedeliverytimeout(this.redeliveryTimeout);
    }

    return recvopts;
  }

  get HTTPFormat() {
    const recvopts = {};
    recvopts.pipe = this.pipe;
    recvopts.autoack = this.autoAck ? 'yes' : null;
    recvopts.block = this.block ? 'yes': null;
    recvopts.count = this.count ? this.count : 1;
    recvopts.timeout = this.timeout ? this.timeount : 60
    recvopts.redeliveryTimeout = this.redeliveryTimeout;
    recvopts.excludeRouting = this.excludeRouting ? 'yes' : null;
    recvopts.excludeRouteLog = this.excludeRouteLog ? 'yes' : null;
    recvopts.excludeDecoratedPayload = this.excludeDecoratedPayload ? 'yes' : null;

    return _.omitBy(recvopts, _.isNil);
  }
}

module.exports = {
  ReceiveOptions,
}