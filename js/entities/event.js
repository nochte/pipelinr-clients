const _ = require('lodash');
const messageEnvelop = require('./messageEnvelop');

class Event {
  #props;
  constructor(props) {
    this.#props = _.reduce(props, (res, val, key) => {
      res[_.camelCase(key)] = val;
      return res;
    }, {});
  }

  get id() {
    return this.#props.id;
  }
  get type() {
    return this.#props.type;
  }
  get createdAt() {
    return this.#props.createdAt;
  }
  get updatedAt() {
    return this.#props.updatedAt;
  }
  get context() {
    return this.#props.context;
  }
  get expiresAt() {
    return this.#props.expiresAt;
  }
  get message() {
    return this.#props.message;
  }
}

const fromHTTPFormat = ({id, Type, CreatedAt, UpdatedAt, Context, ExpiresAt, Message}) => {
  return new Event({
    id: id.value,
    Type,
    CreatedAt: CreatedAt.seconds,
    UpdatedAt: UpdatedAt.seconds,
    Context,
    ExpiresAt: ExpiresAt.seconds,
    Message: messageEnvelop.fromHTTPFormat(Message),
  });
}

const fromGRPCFormat = ({id, message, type, createdat, updatedat, context, expiresat}) => {
  return new Event({
    id: id.value, 
    Type: type, 
    CreatedAt: createdat.seconds, 
    UpdatedAt: updatedat.seconds, 
    Context: context, 
    ExpiresAt: expiresat.seconds,
    Message: messageEnvelop.fromGRPCFormat(message), 
  });
}

module.exports = {
  Event,
  fromHTTPFormat,
  fromGRPCFormat
}