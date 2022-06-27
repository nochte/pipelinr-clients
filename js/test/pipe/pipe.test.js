const { expect } = require('chai');
const _ = require('lodash');
const sleep = require('../../lib/sleep');

const pipe = require('../../pipe');
const randomString = require('../helpers/randomString');

const msgcount = 10;
const pipecount = 3;

[
  pipe.NewGRPCPipe,
  pipe.NewHTTPPipe
].forEach((Pipe, ndx) => {
  describe('Pipe - ' + ndx, () => {
    describe('constructor', () => {
      it('should blow up if no apikey', async () => {
        try {
          Pipe();
          expect(1).eq(0);
        } catch (er) {
          expect(1).eq(1);
        };
      });
    });

    describe('basic interactions', () => {
      let pipe;
      let pipename = randomString(8);
      beforeEach(async () => {
        pipe = Pipe(pipename, process.env.PIPELINR_API_KEY);
      });

      describe('receiveOptions', () => {
        it('shoud set receive opts', async () => {
          expect(pipe.receiveOptions.autoAck).eq(false);
          expect(pipe.receiveOptions.block).eq(false);
          expect(pipe.receiveOptions.pipe).eq(pipename);
          expect(pipe.receiveOptions.count).eq(1);
          expect(pipe.receiveOptions.timeout).eq(0);
          expect(pipe.receiveOptions.redeliveryTimeout).eq(0);

          pipe.receiveOptions = {autoAck: true, count: 5};

          expect(pipe.receiveOptions.autoAck).eq(true);
          expect(pipe.receiveOptions.block).eq(false);
          expect(pipe.receiveOptions.pipe).eq(pipename);
          expect(pipe.receiveOptions.count).eq(5);
          expect(pipe.receiveOptions.timeout).eq(0);
          expect(pipe.receiveOptions.redeliveryTimeout).eq(0);  
        });
      });

      describe('send/recv', () => {
        beforeEach(async () => {
          pipe.receiveOptions = {
            autoAck: true,
            block: true,
            count: 1,
            timeout: 10,
            redeliveryTimeout: 50,
          };
        });

        it('should send one, then receive, ack, log, and complete one', async () => {
          /*  send  */
          const id = await pipe.send({
            Payload: JSON.stringify({foo: 'bar'}),
            Route: [pipename, 'some', 'route'],
          });

          expect(id).not.eq('');
          expect(typeof id).eq('string');

          /*  recv  */
          const elms = await pipe.fetch();
          expect(elms.length).eq(1);

          const elm = elms[0];
          expect(elm.id).eq(id);
          expect(elm.message.route).deep.eq([pipename, 'some', 'route']);
          expect(elm.message.payload.foo).eq('bar');
          expect(elm.message.decoratedPayload.foo).eq('bar');

          /*  ack  */
          const ackres = await pipe.ack(elm.id);
          expect(ackres).eq(true);

          /*  log  */
          const logres = await pipe.log(elm.id, 3, `some message from ${pipename}`);
          expect(logres).eq(true);
          
          /*  addSteps  */
          const stepsres = await pipe.addSteps(id, ['flip', 'flap']);
          expect(stepsres).eq(true);

          /*  decorate  */
          const decres = await pipe.decorate(id, [
            {Key: 'somekey', Value: 'somevalue'},
            {Key: 'otherkey', Value: 'otherval'}
          ]);
          expect(decres).not.eq(undefined);
          expect(decres).not.eq(null);
          expect(decres.length).eq(2);
          expect(decres[0]).eq(true);
          expect(decres[1]).eq(true);

          /*  complete  */
          const completeres = await pipe.complete(id);
          expect(completeres).eq(true);
        });

        it('should omit fields when configured to', async () => {
          pipe.receiveOptions = {
            excludeRouting: true,
            excludeRouteLog: true,
            excludeDecoratedPayload: true,
          }

          const id = await pipe.send({
            Payload: JSON.stringify({foo: 'bar'}),
            Route: [pipename, 'some', 'route'],
          });

          expect(id).not.eq('');
          expect(typeof id).eq('string');

          expect(await pipe.decorate(id, [
            {Key: 'somekey', Value: 'somevalue'},
            {Key: 'otherkey', Value: 'otherval'}
          ])).deep.eq([true, true]);

          const elms = await pipe.fetch();
          expect(elms.length).eq(1);
          const elm = elms[0];
          expect(elm.id).eq(id);
          expect(elm.message.route).deep.eq([])
          expect(elm.message.decoratedPayload).deep.eq({});
          expect(elm.message.routeLog).deep.eq([]);
        });
      });
    });

    describe('flow', () => {
      let pipes = [];
      let pipenamebase = `${randomString(8)}`;
      beforeEach(async () => {
        pipes.push(Pipe(pipenamebase + '-step-1', process.env.PIPELINR_API_KEY));
        pipes.push(Pipe(pipenamebase + '-step-2', process.env.PIPELINR_API_KEY));
        pipes.push(Pipe(pipenamebase + '-step-3', process.env.PIPELINR_API_KEY));

        pipes.forEach((p) => {
          p.receiveOptions = {
            autoAck: false,
            block: false,
            count: 10,
            timeout: 10,
            redeliveryTimeout: 60,
          };

          p.start();
        });
      });

      afterEach(async () => {
        pipes.forEach((p) => {
          p.stop();
        });
      });

      it('should update the message as it goes along', async () => {
        const id = await pipes[0].send({
          Payload: JSON.stringify({foo: 'blip'}),
          Route: [
            pipenamebase + '-step-1',
            pipenamebase + '-step-2',
            pipenamebase + '-step-3',
        ]});

        const stage1 = await pipes[0].next();
        expect(stage1.message.decoratedPayload).deep.eq(stage1.message.payload);
        expect((await pipes[0].ack(stage1.id))).eq(true);
        expect((
          await pipes[0].decorate(
            stage1.id, [{Key: 'stage1', Value: 'yep'}])
            ).length
            ).eq(1);
        expect((await pipes[0].complete(stage1.id))).eq(true);

        const stage2 = await pipes[1].next();
        expect(stage2.message.decoratedPayload.stage1).eq('yep');
        expect((await pipes[1].decorate(stage2.id, [{Key: 'stage1', Value: 'overwritten'}, {Key: 'stage2', Value: 'done'}])).length).eq(2);
        expect((await pipes[1].complete(stage2.id))).eq(true);
        
        const stage3 = await pipes[2].next()
        expect(stage3.message.decoratedPayload.stage1).eq('overwritten');
        expect(stage3.message.decoratedPayload.stage2).eq('done');
        expect((await pipes[2].complete(stage3.id))).eq(true);
      });
    });

    describe('large number of messages and pipes', () => {
      let pipes = [];
      let pipenamebase = `${randomString(8)}`;
      
      let totalShouldProcess = msgcount * pipecount;

      beforeEach(async () => {
        for (let i = 0; i < pipecount; i++) {
          const p = Pipe(`${pipenamebase}-bigstep-${i}`, process.env.PIPELINR_API_KEY)
          p.receiveOptions = {
            autoAck: false,
            block: false,
            count: 50,
            timeout: 1,
            redeliveryTimeout: 10,
          };

          p.start(msgcount);

          pipes.push(p);
        }
      });

      afterEach(async () => {
        pipes.forEach((p) => {
          p.stop();
        });
      });

      it('should process through everything', async () => {
        let totalProcessed = 0;
        let events = {};

        const pushem = async () => {
          const route = pipes.map(p => p.name);

          const topush = [];

          for (let i = 0; i < msgcount; i++) {
            topush.push({
              Payload: JSON.stringify({messagenum: i}),
              Route: route,
            });
            // await pipes[0].send();
          }

          console.log('gonna push some messages', topush.length);

          const work = async () => {
            let pushedcount = 0;
            const st = new Date();
            while(topush.length > 0) {
              const msg = topush.shift();
              await pipes[0].send(msg);
              pushedcount += 1;
            }
            console.log('worker done, messages pushed by this worker', pushedcount, 'in', new Date() - st, 'ms');
          }

          for(let i = 0; i < 10; i++) {
            work();
          }
        }

        pushem();


        while(totalProcessed < totalShouldProcess) {
          // console.log('processed', totalProcessed, 'of', totalShouldProcess, totalProcessed/totalShouldProcess * 100.0, '%');
          for(let pipndx = 0; pipndx < pipes.length; pipndx++) {
            const pipe = pipes[pipndx];
            const thendx = pipndx;
            pipe.next().then((elm) => {
              return pipe.decorate(elm.id, [{Key: `step-${thendx}`, Value: `"${thendx}"`}]).then((res) => {
                return pipe.complete(elm.id).then((res) => {
                  events[elm.id] = elm;
                  totalProcessed++;
                });
              })
            });
          }
          // console.log('sleeping', Object.keys(events).length)
          await sleep(100); 
        }
        
        _.forEach(events, (event, id) => {
          expect(event.message.completedSteps.length).eq(pipes.length-1);
          for(let pipndx = 0; pipndx < pipes.length - 1; pipndx++) {
            expect(event.message.decoratedPayload[`step-${pipndx}`]).eq(`${pipndx}`); 
          }         
        })
      })
    });

    describe('decoration interactions', () => {
      let pipe;
      beforeEach(async () => {
        pipe = Pipe('decoration-interactions', process.env.PIPELINR_API_KEY)
      });

      it('should decorate with keys and subkeys, and expose a way to get specific decorations', async () => {
        const mid = await pipe.send({
          Payload: JSON.stringify({foo: 'blap'}),
          Route: ['decoration-interactions'],
        });

        expect(await pipe.decorate(mid, [
          {Key: 'numeric', Value: `1`},
          {Key: 'flap', Value: 'bar'}, 
          {Key: 'nested.key', Value: 'fff'}, 
          {Key: 'deeply.nested.key', Value: 'fsdf'},
          {Key: 'object', Value: JSON.stringify({
            deep: {
              fields:{
                are: {
                  ok:{
                    with: 'me'
                  }
                }
              }
            }
          })}
        ])).deep.eq([true, true, true, true, true]);

        const decs = await pipe.getDecorations(mid, ['numeric', 'nested.key', 'object']);
        expect(decs.numeric).eq(1);
        expect(decs.nested.key).eq('fff');
        expect(decs.object.deep.fields.are.ok.with).eq('me');
      });
    });
  });
});