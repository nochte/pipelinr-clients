const { expect } = require("chai");
const sleep = require("../../lib/sleep");
const { NewGRPCPipe, NewHTTPPipe } = require("../../pipe");

const worker = require("../../worker");
const randomString = require("../helpers/randomString");

const bigWorkerTestWorkersCount = 5;
const bigWorkerTestMessageCount = 100;

[
  worker.NewGRPCWorker, 
  worker.NewHTTPWorker
].forEach((Worker, ndx) => {
  let workers;
  let pipenamebase;
  let buildWorker;
  let pipes;

  describe("Worker - " + ndx, () => {
    beforeEach(async () => {
      workers = [];
      pipes = [];

      pipenamebase = randomString(8);

      buildWorker = (step) => {
        const st = `${pipenamebase}-${step}`;
        const worker = Worker(st, process.env.PIPELINR_API_KEY);
        worker.receiveOptions = {
          count: 50,
          redeliveryTimeout: 300,
        };
        workers.push(worker);
        pipes.push(NewGRPCPipe(st, process.env.PIPELINR_API_KEY));
        worker.run();
        return worker;
      };
    });
    afterEach(async () => {
      workers.forEach((w) => {
        w.stop();
      });
    });

    describe("all-green workflow", () => {
      let processedMessages = 0;
      let sentMessages = 0;

      beforeEach(async () => {
        for (let i = 0; i < bigWorkerTestWorkersCount; i++) {
          const w = buildWorker(i);
          w.onMessage(async (msg) => {
            await w.pipe.decorate(msg.id, [
              { Key: `worker-${w.step}`, Value: `"${i}"` },
            ]);
            processedMessages++;
          });
        }
      });

      it("should process the messages out", async () => {
        const donepipe = NewGRPCPipe(
          `${pipenamebase}-done`,
          process.env.PIPELINR_API_KEY
        );
        donepipe.receiveOptions = {
          autoAck: true,
          block: true,
          timeout: 10,
          count: 1,
        };
        for (let i = 0; i < bigWorkerTestMessageCount; i++) {
          await pipes[0].send({
            Payload: JSON.stringify({ some: "message" }),
            Route: workers.map((w) => w.name).concat([`${pipenamebase}-done`]),
          });
          sentMessages++;
        }

        while (processedMessages < sentMessages * workers.length) {
          await sleep(100);
        }

        let donemessages = 0;
        while (donemessages < sentMessages) {
          try {
            const msgs = await donepipe.fetch();
            const msg = msgs[0];
            for (let i = 0; i < workers.length; i++) {
              const w = workers[i];
              expect(msg.message.decoratedPayload[`worker-${w.step}`]).eq(
                `${i}`
              );
            }
            donemessages++;
          } catch (er) {
            console.log("got an error trying to finish a job", er);
          }
        }
      });
    });

    describe("fail workflow", () => {
      describe("without an onError", () => {
        let processedMessages = 0;
        beforeEach(async () => {
          const w = buildWorker(0);
          w.onMessage(async (msg) => {
            await w.pipe.decorate(msg.id, [
              { Key: `worker-${w.step}`, Value: `"${0}"` },
            ]);
            processedMessages++;
            throw new Error("app fail");
          });
        });
        it("should complete the messages", async () => {
          const donepipe = NewGRPCPipe(
            `${pipenamebase}-done`,
            process.env.PIPELINR_API_KEY
          );
          donepipe.receiveOptions = {
            autoAck: true,
            block: true,
            timeout: 10,
            count: 1,
          };

          await pipes[0].send({
            Payload: JSON.stringify({ some: "message" }),
            Route: workers.map((w) => w.name).concat([`${pipenamebase}-done`]),
          });

          const sentMessages = 1;

          while (processedMessages < sentMessages * workers.length) {
            await sleep(100);
          }

          let donemessages = 0;
          while (donemessages < sentMessages) {
            try {
              const msgs = await donepipe.fetch();
              const msg = msgs[0];
              for (let i = 0; i < workers.length; i++) {
                const w = workers[i];
                expect(msg.message.decoratedPayload[`worker-${w.step}`]).eq(
                  `${i}`
                );
                expect(msg.message.routeLog.length).eq(2);
                expect(msg.message.routeLog[0].code).eq(-1);
                expect(msg.message.routeLog[1].code).eq(0);
              }
              donemessages++;
            } catch (er) {
              console.log("got an error trying to finish a job", er);
            }
          }
        });
      });

      describe("with an onError", () => {
        let processedMessages = 0;
        beforeEach(async () => {
          const w = buildWorker(0);
          w.onMessage(async (msg) => {
            await w.pipe.decorate(msg.id, [
              { Key: `worker-${w.step}`, Value: `"${0}"` },
            ]);
            throw new Error("app fail");
          });
          w.onError(async (er, msg) => {
            console.log('onerror', er)
            processedMessages++;
            await w.pipe.complete(msg.id);
          });
        });

        it("should not complete the messages", async () => {
          const donepipe = NewGRPCPipe(
            `${pipenamebase}-done`,
            process.env.PIPELINR_API_KEY
          );
          donepipe.receiveOptions = {
            autoAck: true,
            block: true,
            timeout: 10,
            count: 1,
          };

          await pipes[0].send({
            Payload: JSON.stringify({ some: "message" }),
            Route: workers.map((w) => w.name).concat([`${pipenamebase}-done`]),
          });

          const sentMessages = 1;

          while (processedMessages < sentMessages * workers.length) {
            await sleep(100);
          }

          let donemessages = 0;
          while (donemessages < sentMessages) {
            try {
              const msgs = await donepipe.fetch();
              const msg = msgs[0];
              for (let i = 0; i < workers.length; i++) {
                const w = workers[i];
                expect(msg.message.decoratedPayload[`worker-${w.step}`]).eq(
                  `${i}`
                );
                expect(msg.message.routeLog.length).eq(1);
                expect(msg.message.routeLog[0].code).eq(-1);
              }
              donemessages++;
            } catch (er) {
              console.log("got an error trying to finish a job", er);
            }
          }
        });
      });

      describe("with an onError and redelivery", () => {
        let processedMessages = 0;
        beforeEach(async () => {
          const w = buildWorker(0);
          w.receiveOptions = {
            autoAck: false,
            redeliveryTimeout: 10,
            // block: true,
            // timeout: 60,
            count: 10,
          }
          w.onMessage(async (msg) => {
            if(msg.message.decoratedPayload[`worker-${w.step}`]) {
              await w.pipe.decorate(msg.id, [
                {Key: `redelivered`, Value: `yep`}
              ]);
              return true

            }
            await w.pipe.decorate(msg.id, [
              { Key: `worker-${w.step}`, Value: `"${0}"` },
            ]);
            throw new Error("app fail");
          });
          w.onError(async (er, msg) => {
            processedMessages++;
          });
        });

        it("should redeliver the message", async () => {
          const donepipe = NewGRPCPipe(
            `${pipenamebase}-done`,
            process.env.PIPELINR_API_KEY
          );
          donepipe.receiveOptions = {
            autoAck: true,
            block: true,
            timeout: 30,
            count: 1,
          };


          let sentMessages = 0;
          for(let i = 0; i < 10; i++) {
            await pipes[0].send({
              Payload: JSON.stringify({ some: "message" }),
              Route: workers.map((w) => w.name).concat([`${pipenamebase}-done`]),
            });
  
            sentMessages++;
          }


          while (processedMessages < sentMessages * workers.length) {
            await sleep(100);
          }

          let donemessages = 0;
          while (donemessages < sentMessages) {
            try {
              const msgs = await donepipe.fetch();
              const msg = msgs[0];
              for (let i = 0; i < workers.length; i++) {
                const w = workers[i];
                expect(msg.message.decoratedPayload[`worker-${w.step}`]).eq(
                  `${i}`
                );
                expect(msg.message.decoratedPayload['redelivered']).eq('yep');
                expect(msg.message.routeLog.length).eq(2);
                expect(msg.message.routeLog[0].code).eq(-1);
                expect(msg.message.routeLog[1].code).eq(0);
              }
            } catch (er) {
              console.log("got an error trying to finish a job", er);
            }
            donemessages++;
          }
        });
      });

    });
  });
});
