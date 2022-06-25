const _ = require('lodash');

const {
  messages
} = require('@pipelinr/protocol');

class Decorations {
  #props;
  constructor(propsarray) {
    this.#props = _.map(propsarray, (elm) => {
      return _.reduce(elm, (res, val, key) => {
        res[_.camelCase(key)] = val;
        return res
      }, {});
    });
  }

  get GRPCFormat() {
    const deco = new messages.Decorations();
    deco.setDecorationsList(this.#props.map(({key, value}) => {
      const d = new messages.Decoration();
      d.setKey(key);
      d.setValue(value);
      return d;
    }));

    return deco;
  }

  get HTTPFormat() {
    return this.#props.map(({key, value}) => {
      return {Key: key, Value: value};
    });
  }
}

module.exports = {
  Decorations
}