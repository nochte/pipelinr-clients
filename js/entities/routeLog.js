const { messages } = require('@pipelinr/protocol');
const _ = require('lodash');

class RouteLog {
  #props;
  constructor(props) {
    this.#props = _.reduce(props, (res, val, key) => {
      res[_.camelCase(key)] = val;
      return res;
    }, {});
  }

  get step() {
    return this.#props.step;
  }

  get code() {
    return this.#props.code;
  }

  get message() {
    return this.#props.message;
  }

  get time() {
    return this.#props.time;
  }

  get GRPCFormat() {
    const rl = new messages.RouteLog();
    rl.setStep(this.step);
    rl.setCode(this.code);
    rl.setMessage(this.message);
    rl.setTime(this.time);

    return rl;
  }

  get HTTPFormat() {
    return {
      step: this.step,
      code: this.code,
      message: this.message,
      time: this.time
    };
  }
}

const fromHTTPFormat = ({Step, Code, Message, Time}) => {
  return new RouteLog({Step, Code, Message, Time})
}

const fromGRPCFormat = ({step, code, message, time}) => {
  return new RouteLog({
    Step: step,
    Code: code,
    Message: message,
    Time: time,
  });
}

module.exports = {
  RouteLog,
  fromHTTPFormat,
  fromGRPCFormat
}