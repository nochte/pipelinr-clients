package worker

import (
	"errors"
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nochte/pipelinr-clients/go/lib"
	"github.com/nochte/pipelinr-clients/go/pipe"
	"github.com/nochte/pipelinr-protocol/protobuf/messages"
	"github.com/nochte/pipelinr-protocol/protobuf/pipes"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/tidwall/gjson"
)

const bigWorkerTestWorkersCount = 3
const bigWorkerTestMessageCount = 25

func TestWorker(t *testing.T) {
	Convey("Worker tests", t, func() {
		type TestDefinition struct {
			getpipe func(step string) *pipe.Pipe
			name    string
		}
		for _, td := range []TestDefinition{
			{name: "grpc worker", getpipe: func(step string) *pipe.Pipe { return pipe.NewGRPC("", "", step) }},
			{name: "http worker", getpipe: func(step string) *pipe.Pipe { return pipe.NewHTTP("", "", step) }},
		} {
			Convey(td.name, func() {

				workers := make([]*Worker, 0)
				ppipes := make([]*pipe.Pipe, 0)
				pipenamebase := lib.GenerateRandomString(8)

				buildworker := func(step string) *Worker {
					st := fmt.Sprintf("%v-%v", pipenamebase, step)
					p := td.getpipe(st)
					worker := New(p)
					i3250 := int32(50)
					i64300 := int64(300)
					worker.SetReceiveOptions(&i3250, nil, &i64300, nil, nil, nil, nil, nil)
					workers = append(workers, worker)
					ppipes = append(ppipes, p)
					go worker.Run()
					return worker
				}

				Reset(func() {
					for ndx := range workers {
						workers[ndx].Stop()
					}
					workers = make([]*Worker, 0)
					ppipes = make([]*pipe.Pipe, 0)
				})

				Convey("all-green workflow", func() {
					processedMessages := uint64(0)
					sentMessages := uint64(0)

					for i := 0; i < bigWorkerTestWorkersCount; i++ {
						func(nn int) {
							w := buildworker(strconv.Itoa(nn))
							w.OnMessage(func(msg *messages.Event, p *pipe.Pipe) error {
								atomic.AddUint64(&processedMessages, 1)
								ers := p.Decorate(msg.GetStringId(), []*pipes.Decoration{
									{Key: fmt.Sprintf("worker-%v", w.Step()), Value: fmt.Sprintf(`"%v"`, nn)},
								})
								for _, er := range ers {
									if er != nil {
										return er
									}
								}
								return nil
							})

						}(i)
					}

					donepipe := td.getpipe(fmt.Sprintf("%v-done", pipenamebase))
					i6410 := int64(10)
					i321 := int32(1)
					btrue := true
					donepipe.SetReceiveOptions(&i321, &i6410, nil, &btrue, &btrue, nil, nil, nil)

					route := make([]string, 0, len(workers))
					for ndx := range workers {
						route = append(route, workers[ndx].Name())
					}
					for i := 0; i < bigWorkerTestMessageCount; i++ {
						ppipes[0].Send(`{"some":"message"}`, append(route, fmt.Sprintf("%v-done", pipenamebase)))
						sentMessages++
					}

					for int(processedMessages) < int(sentMessages)*len(workers) {
						time.Sleep(time.Millisecond * 100)
					}

					donemessages := 0
					for donemessages < int(sentMessages) {
						msgs, er := donepipe.Fetch()
						if er != nil {
							Println("error getting messages from donepipe", er)
							continue
						}
						for _, msg := range msgs {
							for i := range workers {
								worker := workers[i]
								So(gjson.Get(msg.GetMessage().GetDecoratedPayload(), fmt.Sprintf("worker-%v", worker.Step())).String(), ShouldEqual, fmt.Sprintf("%v", i))
							}
							donemessages++
							donepipe.Complete(msg.GetStringId())
						}
					}
				})
				Convey("fail workflow", func() {
					Convey("without an onError", func() {
						processedMessages := uint64(0)

						w := buildworker(strconv.Itoa(0))
						w.OnMessage(func(msg *messages.Event, p *pipe.Pipe) error {
							p.Decorate(msg.GetStringId(), []*pipes.Decoration{
								{Key: fmt.Sprintf("worker-%v", w.Step()), Value: `"0"`},
							})
							atomic.AddUint64(&processedMessages, 1)
							return errors.New("app fail")
						})

						donepipe := td.getpipe(fmt.Sprintf("%v-done", pipenamebase))
						i6410 := int64(10)
						i321 := int32(1)
						btrue := true
						donepipe.SetReceiveOptions(&i321, &i6410, nil, &btrue, &btrue, nil, nil, nil)

						route := make([]string, 0, len(workers))
						for ndx := range workers {
							route = append(route, workers[ndx].Name())
						}

						ppipes[0].Send(`{"some":"message"}`, append(route, fmt.Sprintf("%v-done", pipenamebase)))

						for processedMessages < 1 {
							time.Sleep(time.Millisecond * 100)
						}

						donemessages := 0
						for donemessages < 1 {
							msgs, er := donepipe.Fetch()
							if er != nil {
								Println("error getting messages from donepipe", er)
								continue
							}
							for _, msg := range msgs {
								for i := range workers {
									worker := workers[i]
									So(gjson.Get(msg.GetMessage().GetDecoratedPayload(), fmt.Sprintf("worker-%v", worker.Step())).String(), ShouldEqual, fmt.Sprintf("%v", i))
									So(len(msg.GetMessage().GetRouteLog()), ShouldEqual, 2)
									So(msg.GetMessage().GetRouteLog()[0].GetCode(), ShouldEqual, -1)
									So(msg.GetMessage().GetRouteLog()[1].GetCode(), ShouldEqual, 0)
								}
								donemessages++
								donepipe.Complete(msg.GetStringId())
							}
						}
					})

					Convey("with an onError", func() {
						processedMessages := uint64(0)

						w := buildworker(strconv.Itoa(0))
						w.OnMessage(func(msg *messages.Event, p *pipe.Pipe) error {
							p.Decorate(msg.GetStringId(), []*pipes.Decoration{
								{Key: fmt.Sprintf("worker-%v", w.Step()), Value: `"0"`},
							})
							return errors.New("app fail")
						})
						w.OnError(func(msg *messages.Event, er error, p *pipe.Pipe) {
							atomic.AddUint64(&processedMessages, 1)
							p.Complete(msg.GetStringId())
						})

						donepipe := td.getpipe(fmt.Sprintf("%v-done", pipenamebase))
						i6410 := int64(10)
						i321 := int32(1)
						btrue := true
						donepipe.SetReceiveOptions(&i321, &i6410, nil, &btrue, &btrue, nil, nil, nil)

						route := make([]string, 0, len(workers))
						for ndx := range workers {
							route = append(route, workers[ndx].Name())
						}

						ppipes[0].Send(`{"some":"message"}`, append(route, fmt.Sprintf("%v-done", pipenamebase)))

						for processedMessages < 1 {
							time.Sleep(time.Millisecond * 100)
						}

						donemessages := 0
						for donemessages < 1 {
							msgs, er := donepipe.Fetch()
							if er != nil {
								Println("error getting messages from donepipe", er)
								continue
							}
							for _, msg := range msgs {
								for i := range workers {
									worker := workers[i]
									So(gjson.Get(msg.GetMessage().GetDecoratedPayload(), fmt.Sprintf("worker-%v", worker.Step())).String(), ShouldEqual, fmt.Sprintf("%v", i))
									So(len(msg.GetMessage().GetRouteLog()), ShouldEqual, 1)
									So(msg.GetMessage().GetRouteLog()[0].GetCode(), ShouldEqual, -1)
								}
								donemessages++
								donepipe.Complete(msg.GetStringId())
							}
						}
					})

					Convey("with an onError and redelivery", func() {
						processedMessages := uint64(0)

						w := buildworker(strconv.Itoa(0))
						i3210 := int32(10)
						bfalse := false
						i6410 := int64(10)
						w.SetReceiveOptions(&i3210, nil, &i6410, &bfalse, nil, nil, nil, nil)
						w.OnMessage(func(msg *messages.Event, p *pipe.Pipe) error {
							if gjson.Get(msg.GetMessage().GetDecoratedPayload(), fmt.Sprintf("worker-%v", w.Step())).String() != "" {
								p.Decorate(msg.GetStringId(), []*pipes.Decoration{
									{Key: "redelivered", Value: "yep"},
								})
								p.Complete(msg.GetStringId())
								atomic.AddUint64(&processedMessages, 1)
								return nil
							}
							p.Decorate(msg.GetStringId(), []*pipes.Decoration{
								{Key: fmt.Sprintf("worker-%v", w.Step()), Value: `"0"`},
							})
							return errors.New("app fail")
						})
						w.OnError(func(msg *messages.Event, er error, p *pipe.Pipe) {
							// intentional no-op
						})

						donepipe := td.getpipe(fmt.Sprintf("%v-done", pipenamebase))
						i321 := int32(1)
						btrue := true
						donepipe.SetReceiveOptions(&i321, &i6410, nil, &btrue, &btrue, nil, nil, nil)

						route := make([]string, 0, len(workers))
						for ndx := range workers {
							route = append(route, workers[ndx].Name())
						}

						ppipes[0].Send(`{"some":"message"}`, append(route, fmt.Sprintf("%v-done", pipenamebase)))

						for processedMessages < 1 {
							time.Sleep(time.Millisecond * 100)
						}

						donemessages := 0
						for donemessages < 1 {
							msgs, er := donepipe.Fetch()
							if er != nil {
								Println("error getting messages from donepipe", er)
								continue
							}
							for _, msg := range msgs {
								for i := range workers {
									worker := workers[i]
									So(gjson.Get(msg.GetMessage().GetDecoratedPayload(), fmt.Sprintf("worker-%v", worker.Step())).String(), ShouldEqual, fmt.Sprintf("%v", i))
									So(gjson.Get(msg.GetMessage().GetDecoratedPayload(), "redelivered").String(), ShouldEqual, "yep")
									So(len(msg.GetMessage().GetRouteLog()), ShouldEqual, 2)
									So(msg.GetMessage().GetRouteLog()[0].GetCode(), ShouldEqual, -1)
									So(msg.GetMessage().GetRouteLog()[1].GetCode(), ShouldEqual, 0)
								}
								donemessages++
								donepipe.Complete(msg.GetStringId())
							}
						}
					})
				})
			})
		}
	})
}
