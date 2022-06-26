const { expect } = require('chai');
const _ = require('lodash');
const { ReceiveOptions } = require('../../entities/receiveOptions');
const { RouteLog } = require('../../entities/routeLog');
const { Decorations } = require('../../entities/decorations');

const randomString = require('../helpers/randomString');

const GRPC = require('../../pipe/drivers/grpc');
const HTTP = require('../../pipe/drivers/http');
[
  GRPC,
  HTTP
].forEach((Driver, ndx) => {
  describe('Driver - ' + ndx, () => {
    let driver = new Driver(process.env.PIPELINR_API_KEY)

    describe('send', () => {
      it('should return an id', async () => {
        const res = await driver.send({
          Payload: JSON.stringify({foo: 'bar'}),
          Route: ['blackhole'],
        });
        expect(typeof res).eq('string');
        expect(res.length).greaterThan(1);
      });
    });

    describe('message has been sent', () => {
      let messageId;
      let route;

      beforeEach(async () => {
        route = [randomString(8), randomString(8)];
        messageId = await driver.send({
          Payload: JSON.stringify({foo: 'bar'}),
          Route: route
        });
      });

      afterEach(async () => {
        for(let i = 0; i < route.length; i++) {
          try {
            await driver.complete(messageId, route[i]);
          } catch (er) {
            // no-op
            // console.log('no cmpl', route[i], messageId);
          }
        }
      });

      describe('recv', () => {
        it('should return an event in the proper format', async () => {
          const [event] = await driver.recv(new ReceiveOptions({
            pipe: route[0],
            count: 1,
            block: true,
            autoAck: true
          }));
          expect(event.message.payload).deep.eq({foo: 'bar'});
          expect(event.message.decoratedPayload).deep.eq({foo: 'bar'});
          expect(event.message.route).deep.eq(route);
        });
      });

      describe('message has been received', () => {
        let event;

        beforeEach(async () => {
          const events = await driver.recv(new ReceiveOptions({
            pipe: route[0],
            count: 1,
            block: true,
            autoAck: true
          }));
          event = events[0];
        });


        describe('ack', () => {
          it('should return true on good, unacknowledged message', async () => {
            expect(await driver.ack(event.id, route[0])).eq(true);
          });
          it('should return true on good, acknowledged message', async () => {
            expect(await driver.ack(event.id, route[0])).eq(true);
            expect(await driver.ack(event.id, route[0])).eq(true);
          });
          it('should return true on bad id', async () => {
            expect(await driver.ack('badid', route[0])).eq(true);
          });
        });
        describe('complete', () => {
          it('should return true on good, incomplete message', async () => {
            expect(await driver.complete(event.id, route[0])).eq(true);
          });
          it('should return false on good, complete message', async () => {
            expect(await driver.complete(event.id, route[0])).eq(true);
            expect(await driver.complete(event.id, route[0])).eq(false);
          });
          it('should throw error on a message not found', async () => {
            try {
              await driver.complete('badid', route[0]);
              expect(1).eq(2);
            } catch (er) {
              expect(1).eq(1);
            }
          });

        });
        describe('appendLog', () => {
          it('should append the log', async () => {
            expect(await driver.appendLog(event.id, new RouteLog({
              step: route[0],
              code: 33,
              message: 'some message here'
            }))).eq(true);
            await driver.complete(event.id, route[0]);
            const [msg] = await driver.recv(new ReceiveOptions({
              pipe: route[1],
              count: 1,
              block: true,
              autoAck: true
            }));

            expect(msg.message.routeLog.length).eq(1);
            expect(msg.message.routeLog[0].step).eq(route[0]);
            expect(msg.message.routeLog[0].code).eq(33);
            expect(msg.message.routeLog[0].message).eq('some message here');
          })
        });
        describe('addSteps', () => {
          it('should add steps right after this one', async () => {
            expect(await driver.addStepsAfter(event.id, route[0], ['added', 'after'])).eq(true);
            await driver.complete(event.id, route[0]);
            const [msg] = await driver.recv(new ReceiveOptions({
              pipe: 'added',
              count: 1,
              block: true,
              autoAck: true
            }));

            expect(msg.message.route[2]).eq('after');
            expect(msg.message.route[3]).eq(route[1]);
            await driver.complete(event.id, 'added');
            await driver.complete(event.id, 'after');
          });
        });
        describe('decorate', () => {
          it('should decorate the payload', async () => {
            const res = await driver.decorate(event.id, new Decorations(
              [
                {Key: 'foo', value: 'bar'},
                {key: 'flip', Value: 'fleeeep'}
              ]
            ));
            expect(res.length).eq(2);
            expect(res[0]).eq(true);
            expect(res[1]).eq(true);

            await driver.complete(event.id, route[0]);
            const [msg] = await driver.recv(new ReceiveOptions({
              pipe: route[1],
              count: 1,
              block: true,
              autoAck: true
            }));
  
            expect(msg.message.decoratedPayload.foo).eq('bar');
            expect(msg.message.decoratedPayload.flip).eq('fleeeep');  

            const decs = await driver.getDecorations(event.id, ['foo', 'bar', 'flip']);
            expect(decs.foo).eq('bar');
            expect(decs.flip).eq('fleeeep');
            expect(decs.bar).eq(undefined);
          });
        });  
      });
    });
  });
});