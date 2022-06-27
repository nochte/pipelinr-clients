package pipe

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/nochte/pipelinr-clients/go/lib"
	"github.com/nochte/pipelinr-clients/go/pipe/drivers"
	"github.com/nochte/pipelinr-protocol/protobuf/messages"
	"github.com/nochte/pipelinr-protocol/protobuf/pipes"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/tidwall/gjson"
)

func TestPipe(t *testing.T) {
	msgcount := 11
	pipecount := 4

	Convey("Pipe tests", t, func() {
		type TestDefinition struct {
			getdriver func() drivers.Driver
			name      string
		}
		for _, td := range []TestDefinition{
			{name: "grpc pipe", getdriver: func() drivers.Driver {
				return drivers.NewGRPCDriver("", "")
			}},
			{name: "http driver", getdriver: func() drivers.Driver {
				return drivers.NewHTTPDriver("", "")
			}},
		} {
			Convey(td.name, func() {
				driver := td.getdriver()

				Convey("basic interactions", func() {
					pipename := lib.GenerateRandomString(8)
					pipe := New(driver, pipename)

					Convey("receiveOptions", func() {
						So(pipe.ReceiveOptions().GetAutoAck(), ShouldEqual, false)
						So(pipe.ReceiveOptions().GetBlock(), ShouldEqual, false)
						So(pipe.ReceiveOptions().GetPipe(), ShouldEqual, pipename)
						So(pipe.ReceiveOptions().GetCount(), ShouldEqual, 1)
						So(pipe.ReceiveOptions().GetTimeout(), ShouldEqual, 0)
						So(pipe.ReceiveOptions().GetRedeliveryTimeout(), ShouldEqual, 0)

						tr := true
						ct := int32(5)
						pipe.SetReceiveOptions(&ct, nil, nil, &tr, nil, nil, nil, nil)

						So(pipe.ReceiveOptions().GetAutoAck(), ShouldEqual, true)
						So(pipe.ReceiveOptions().GetBlock(), ShouldEqual, false)
						So(pipe.ReceiveOptions().GetPipe(), ShouldEqual, pipename)
						So(pipe.ReceiveOptions().GetCount(), ShouldEqual, 5)
						So(pipe.ReceiveOptions().GetTimeout(), ShouldEqual, 0)
						So(pipe.ReceiveOptions().GetRedeliveryTimeout(), ShouldEqual, 0)
					})
					Convey("send/recv", func() {
						tru := true
						one := int32(1)
						ten := int64(10)
						fifty := int64(50)
						pipe.SetReceiveOptions(&one, &ten, &fifty, &tru, &tru, nil, nil, nil)

						id, er := pipe.Send(`{"foo":"bar"}`, []string{pipename, "some", "route"})
						So(er, ShouldBeNil)
						So(len(id), ShouldBeGreaterThan, 1)

						// recv
						elms, er := pipe.Fetch()
						So(er, ShouldBeNil)
						So(len(elms), ShouldEqual, 1)
						elm := elms[0]
						So(elm.GetStringId(), ShouldEqual, id)
						So(elm.GetMessage().GetRoute(), ShouldResemble, []string{pipename, "some", "route"})
						So(gjson.Get(elm.GetMessage().GetPayload(), "foo").String(), ShouldEqual, "bar")
						So(gjson.Get(elm.GetMessage().GetDecoratedPayload(), "foo").String(), ShouldEqual, "bar")

						// ack
						So(pipe.Ack(id), ShouldBeNil)

						// log
						So(pipe.Log(id, 3, fmt.Sprintf("some message from %v", pipename)), ShouldBeNil)

						// addsteps
						So(pipe.AddSteps(id, []string{"flip", "flap"}), ShouldBeNil)

						// decorate
						res := pipe.Decorate(id, []*pipes.Decoration{
							{Key: "somekey", Value: "somevalue"},
							{Key: "otherkey", Value: "otherval"}})

						So(len(res), ShouldEqual, 2)
						So(res[0], ShouldBeNil)
						So(res[1], ShouldBeNil)

						// complete
						So(pipe.Complete(id), ShouldBeNil)
					})
				})
				Convey("flow", func() {
					pipenamebase := lib.GenerateRandomString(8)
					ppipes := []*Pipe{
						New(driver, fmt.Sprintf("%v-step-1", pipenamebase)),
						New(driver, fmt.Sprintf("%v-step-2", pipenamebase)),
						New(driver, fmt.Sprintf("%v-step-3", pipenamebase)),
					}
					for _, p := range ppipes {
						i32ten := int32(10)
						i64ten := int64(10)
						i64sixty := int64(60)
						p.SetReceiveOptions(&i32ten, &i64ten, &i64sixty, nil, nil, nil, nil, nil)

						go p.Start(0)
					}

					Reset(func() {
						for _, p := range ppipes {
							p.Stop()
						}
					})

					_, er := ppipes[0].Send(`{"foo":"blip"}`, []string{
						fmt.Sprintf("%v-step-1", pipenamebase),
						fmt.Sprintf("%v-step-2", pipenamebase),
						fmt.Sprintf("%v-step-3", pipenamebase)})
					So(er, ShouldBeNil)

					stage1 := <-ppipes[0].Chan()
					So(stage1.GetMessage().GetDecoratedPayload(), ShouldEqual, `{"foo":"blip"}`)
					So(ppipes[0].Ack(stage1.GetStringId()), ShouldBeNil)

					decres := ppipes[0].Decorate(stage1.GetStringId(), []*pipes.Decoration{
						{Key: "stage1", Value: "yep"}})
					So(decres[0], ShouldBeNil)

					So(ppipes[0].Complete(stage1.GetStringId()), ShouldBeNil)

					stage2 := <-ppipes[1].Chan()
					So(gjson.Get(stage2.GetMessage().GetDecoratedPayload(), "stage1").String(), ShouldEqual, "yep")
					So(ppipes[1].Decorate(stage2.GetStringId(), []*pipes.Decoration{
						{Key: "stage1", Value: "overwritten"},
						{Key: "stage2", Value: "done"}}), ShouldResemble, []error{nil, nil})
					So(ppipes[1].Complete(stage2.GetStringId()), ShouldBeNil)

					stage3 := <-ppipes[2].Chan()
					So(gjson.Get(stage3.GetMessage().GetDecoratedPayload(), "stage1").String(), ShouldEqual, "overwritten")
					So(gjson.Get(stage3.GetMessage().GetDecoratedPayload(), "stage2").String(), ShouldEqual, "done")
					So(ppipes[2].Complete(stage3.GetStringId()), ShouldBeNil)
				})
				Convey("large number of messages and pipes", func() {
					pipenamebase := lib.GenerateRandomString(8)
					// totalShouldProcess := msgcount * pipecount

					// setup pipes
					ppipes := make([]*Pipe, 0, pipecount)
					i32fifty := int32(50)
					i64ten := int64(10)
					for i := 0; i < pipecount; i++ {
						p := New(driver, fmt.Sprintf("%v-bigstep-%v", pipenamebase, i))
						p.SetReceiveOptions(&i32fifty, nil, &i64ten, nil, nil, nil, nil, nil)
						go p.Start(0)

						ppipes = append(ppipes, p)
					}

					topush := make(chan *messages.MessageEnvelop, msgcount)
					pushwg := sync.WaitGroup{}
					for i := 0; i < 1; i++ {
						pushwg.Add(1)

						go func(ndx int) {
							defer pushwg.Done()
							startTime := time.Now()
							pushcount := 0
							for msg := range topush {
								ppipes[0].Send(msg.GetPayload(), msg.GetRoute())
								pushcount++
							}
							fmt.Println(ndx, "worker done - messages pushed by this worker", pushcount, "in", time.Since(startTime))
						}(i)
					}

					route := make([]string, 0, len(ppipes))
					for _, p := range ppipes {
						route = append(route, p.Name())
					}

					for i := 0; i < msgcount; i++ {
						topush <- &messages.MessageEnvelop{
							Route:   route,
							Payload: fmt.Sprintf(`{"messagenum":"%v"}`, i)}
					}
					close(topush)

					// Println("messages enqueued, waiting til done")
					// pushwg.Wait()
					// Println("done pushing")

					totalShouldProcess := pipecount * msgcount

					elmchan := make(chan *messages.Event, totalShouldProcess)

					pwg := sync.WaitGroup{}
					for pndx := range ppipes {
						pwg.Add(1)
						go func(ppipe *Pipe, ndx int) {
							defer pwg.Done()
							ch := ppipe.Chan()
							for elm := range ch {
								// fmt.Println(ppipe.Name(), "got a message", elm.Message.Payload)
								ers := ppipe.Decorate(elm.GetStringId(), []*pipes.Decoration{
									{Key: fmt.Sprintf("step-%v", ndx), Value: fmt.Sprintf(`"%v"`, ndx)}})
								for _, er := range ers {
									if er != nil {
										fmt.Println(ppipe.Name(), "error decorating", er.Error())
									}
								}
								if er := ppipe.Complete(elm.GetStringId()); er != nil {
									fmt.Println(ppipe.Name(), "error completing", er.Error())
								}
								elmchan <- elm
							}
						}(ppipes[pndx], pndx)
					}

					events := make(map[string]*messages.Event)
					totalProcessed := 0
					go func() {
						for elm := range elmchan {
							events[elm.GetStringId()] = elm
							totalProcessed++
						}
					}()

					for totalProcessed < totalShouldProcess {
						time.Sleep(time.Millisecond * 250)
					}
					for pndx := range ppipes {
						ppipes[pndx].Stop()
					}

					Println("all done setting up, waiting for processing to complete")
					pwg.Wait()
					close(elmchan)

					for _, evt := range events {
						So(len(evt.GetMessage().GetCompletedSteps()), ShouldEqual, len(ppipes)-1)

						for ndx := 0; ndx < len(ppipes)-1; ndx++ {
							So(gjson.Get(evt.GetMessage().GetDecoratedPayload(), fmt.Sprintf("step-%v", ndx)).String(), ShouldEqual, fmt.Sprintf(`%v`, ndx))
						}
					}
				})
			})
		}
	})
}
