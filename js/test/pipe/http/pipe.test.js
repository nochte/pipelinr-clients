const Pipe = require('../../../pipe/http');
var assert = require('assert');

describe('Pipe', () => {
  describe('constructor', () => {
    it('should blow up if no apikey', async () => {
      try {
        new Pipe();
        assert.equal(1, 0);
      } catch (er) {
        assert.equal(1, 1);
      }
    });
  });
  describe('workflow', () => {
    let pipe;
    before(async () => {
      pipe = new Pipe(process.env.PIPELINR_API_KEY, 'testpipe');
    });

    describe('setReceiveOptions', () => {
      it('shoud set receive opts', async () => {
        assert.equal(pipe._receiveOptions.autoAck, false);
        assert.equal(pipe._receiveOptions.block, false);
        assert.equal(pipe._receiveOptions.pipe, 'testpipe');
        assert.equal(pipe._receiveOptions.count, 1);
        assert.equal(pipe._receiveOptions.timeout, 0);
        assert.equal(pipe._receiveOptions.redeliveryTimeout, 0);
        pipe.setReceiveOptions({autoAck: true, count: 5});
        assert.equal(pipe._receiveOptions.autoAck, true);
        assert.equal(pipe._receiveOptions.block, false);
        assert.equal(pipe._receiveOptions.pipe, 'testpipe');
        assert.equal(pipe._receiveOptions.count, 5);
        assert.equal(pipe._receiveOptions.timeout, 0);
        assert.equal(pipe._receiveOptions.redeliveryTimeout, 0);  
      });
    });

    describe('send', () => {
      it('should return an id', async () => {
        try {
          const res = await pipe.send({
            Payload: "{foo: 'bar'}",
            Route: [pipe._receiveOptions.pipe, 'some', 'route'],
          });
          assert.notEqual(res, "");
          assert.notEqual(res, null);
          assert.notEqual(res, undefined);
        } catch (er) {
          assert.equal(1, 0, er);
        }
      }).timeout(30000);

      describe('_fetchWithBackoff', () => {
        it('should fetch some messages', async () => {
          try {
            const res = await pipe._fetchWithBackoff();
          } catch (er) {
            assert.equal(1, 0, er);
          }  
        }).timeout(30000);
      });
    });
  });
});