const _ = require('lodash');
const routeLog = require('./routeLog');

class MessageEnvelop {
  #props;
  constructor(props) {
    this.#props = _.reduce(props, (res, val, key) => {
      res[_.camelCase(key)] = val;
      return res;
    }, {});
  }

  get payloadRaw() {
    return this.#props.payload;
  }

  get payload() {
    return JSON.parse(this.#props.payload);
  }

  get decoratedPayloadRaw() {
    return this.#props.decoratedPayload;
  }

  get decoratedPayload() {
    return JSON.parse(this.#props.decoratedPayload);
  }

  get route() {
    return this.#props.route;
  }

  get completedSteps() {
    return this.#props.completedSteps;
  }

  get routeLog() {
    return this.#props.routeLog;
  }
}

const fromHTTPFormat = ({Payload, Route, CompletedSteps, RouteLog, DecoratedPayload}) => {
  return new MessageEnvelop({
    Payload,
    Route,
    CompletedSteps: (CompletedSteps || []),
    RouteLog: (RouteLog || []).map(routeLog.fromHTTPFormat),
    DecoratedPayload
  });
}

const fromGRPCFormat = ({payload, routeList, completedstepsList, routelogList, decoratedpayload}) => {
  return new MessageEnvelop({
    Payload: payload, 
    Route: routeList, 
    CompletedSteps: (completedstepsList || []), 
    RouteLog: (routelogList || []).map(routeLog.fromGRPCFormat), 
    DecoratedPayload: decoratedpayload,
  });
}

module.exports = {
  MessageEnvelop,
  fromHTTPFormat,
  fromGRPCFormat,
}