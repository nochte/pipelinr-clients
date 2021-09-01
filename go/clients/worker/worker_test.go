package worker

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"testing"

	protomessages "github.com/nochte/pipelinr-protocol/protobuf/messages"
	protopipes "github.com/nochte/pipelinr-protocol/protobuf/pipes"
	embeddedpipes "github.com/nochte/pipelinr/clients/pipes/embeddedservice"
	grpcpipes "github.com/nochte/pipelinr/clients/pipes/grpc-micro-service"
	repositories "github.com/nochte/pipelinr/repositories/inmemory"
	"github.com/nochte/pipelinr/services"
	. "github.com/smartystreets/goconvey/convey"
)

func TestWorkerInternalService(t *testing.T) {
	Convey("worker interaction", t, func() {
		repo := repositories.DefaultInMemoryEventRepository()
		redelivery := repositories.NewRedeliveryCache()
		accounting := repositories.DefaultInMemoryEventAccountingRepository()
		pipesvc := services.NewPipeService(repo, accounting, redelivery, os.Getenv("RABBITMQ_URL"))
		pipe := embeddedpipes.New(pipesvc, &protopipes.ReceiveOptions{})

		Convey("two workers, with decorations", func() {
			worker1 := New(pipe, "two-workers-first", 0, true, true)
			pipe2 := embeddedpipes.New(pipesvc, &protopipes.ReceiveOptions{})
			worker2 := New(pipe2, "two-workers-second", 0, true, true)
			worker1.OnMessage(func(in *protomessages.Event, w *Worker) error {
				jsonval, _ := json.Marshal(map[string]interface{}{
					"something": "goes",
					"here":      true,
				})
				return w.Decorate([]*protopipes.Decoration{
					&protopipes.Decoration{
						Key:   "other",
						Value: `4`,
					},
					&protopipes.Decoration{
						Key:   "nested.key",
						Value: `9.17`,
					},
					&protopipes.Decoration{
						Key:   "someother.key",
						Value: string(jsonval),
					},
					&protopipes.Decoration{
						Key:   "some",
						Value: `"value"`,
					}})
			})
			worker2.OnMessage(func(in *protomessages.Event, w *Worker) error {
				var parsedout map[string]interface{}
				expectedout := map[string]interface{}{
					"other": 4,
					"nested": map[string]interface{}{
						"key": 9.17,
					},
					"someother": map[string]interface{}{
						"key": map[string]interface{}{
							"something": "goes",
							"here":      true,
						},
					},
					"some": "value",
				}
				er := json.Unmarshal([]byte(in.GetMessage().GetDecoratedPayload()), &parsedout)
				if !reflect.DeepEqual(fmt.Sprintf("%v", parsedout), fmt.Sprintf("%v", expectedout)) {
					t.Logf("mismatch %+v, expected %+v", parsedout, expectedout)
					So(parsedout, ShouldResemble, expectedout)
				}
				// So(parsedout, ShouldResemble, expectedout)
				return er
			})

			xid, er := pipe.Send(&protomessages.MessageEnvelop{
				Payload: "",
				Route:   []string{"two-workers-first", "two-workers-second"},
			})
			So(er, ShouldBeNil)
			So(xid, ShouldNotEqual, "")
			er = pipe.LoadNext()
			So(er, ShouldBeNil)
			worker1.runOne(pipe.CurrentMessage())
			er = pipe2.LoadNext()
			So(er, ShouldBeNil)
			worker2.runOne(pipe2.CurrentMessage())
		})
		Convey("base case, no callbacks", func() {
			worker := New(pipe, "first", 0, false, false)

			Convey("successful message", func() {
				success := false
				worker.OnMessage(func(in *protomessages.Event, w *Worker) error {
					success = true
					return nil
				})
				xid, er := pipe.Send(&protomessages.MessageEnvelop{
					Payload: `{"some":"value"}`,
					Route:   []string{"first"},
				})
				So(er, ShouldBeNil)
				So(xid, ShouldNotEqual, "")
				Convey("with no on success callback", func() {
					er = pipe.LoadNext()
					So(er, ShouldBeNil)
					worker.runOne(pipe.CurrentMessage())
					So(success, ShouldBeTrue)
				})

				Convey("with an on success callback", func() {
					supersuccess := false
					worker.OnSuccess(func(in *protomessages.Event, w *Worker) error {
						supersuccess = true
						return nil
					})
					er = pipe.LoadNext()
					So(er, ShouldBeNil)
					worker.runOne(pipe.CurrentMessage())
					So(supersuccess, ShouldBeTrue)
				})
			})

			Convey("failure message", func() {
				worker.OnMessage(func(in *protomessages.Event, w *Worker) error {
					return errors.New("bad juju")
				})
				xid, er := pipe.Send(&protomessages.MessageEnvelop{
					Payload: `{"some":"value"}`,
					Route:   []string{"first"},
				})
				So(er, ShouldBeNil)
				So(xid, ShouldNotEqual, "")

				Convey("with no on failure callback", func() {
					er = pipe.LoadNext()
					So(er, ShouldBeNil)
					worker.runOne(pipe.CurrentMessage())
				})

				Convey("with an on failure callback", func() {
					superfail := false
					worker.OnFailure(func(in *protomessages.Event, w *Worker) error {
						w.Log(in.GetStringId(), -99, "fleep")
						w.AddStepAfterThis([]string{"badstep"})
						w.Decorate([]*protopipes.Decoration{
							&protopipes.Decoration{
								Key: "some",
								// Value: structpb.NewStringValue("value"),
								Value: `"value"`,
							}})
						superfail = true
						return w.Complete(in.GetStringId())
					})
					er = pipe.LoadNext()
					So(er, ShouldBeNil)
					worker.runOne(pipe.CurrentMessage())
					So(superfail, ShouldBeTrue)
				})
			})
		})
	})
}

func TestWorkerGRPCService(t *testing.T) {
	if os.Getenv("NO_GRPC") != "" {
		return
	}
	Convey("worker interaction", t, func() {
		pipe := grpcpipes.New("pipelinr.pipe", &protopipes.ReceiveOptions{})

		Convey("two workers, with decorations", func() {
			worker1 := New(pipe, "two-workers-first", 0, true, true)
			pipe2 := grpcpipes.New("pipelinr.pipe", &protopipes.ReceiveOptions{})
			worker2 := New(pipe2, "two-workers-second", 0, true, true)
			worker1.OnMessage(func(in *protomessages.Event, w *Worker) error {
				jsonval, _ := json.Marshal(map[string]interface{}{
					"something": "goes",
					"here":      true,
				})
				return w.Decorate([]*protopipes.Decoration{
					&protopipes.Decoration{
						Key:   "other",
						Value: `4`,
					},
					&protopipes.Decoration{
						Key:   "nested.key",
						Value: `9.17`,
					},
					&protopipes.Decoration{
						Key:   "someother.key",
						Value: string(jsonval),
					},
					&protopipes.Decoration{
						Key:   "some",
						Value: `"value"`,
					}})
			})
			worker2.OnMessage(func(in *protomessages.Event, w *Worker) error {
				var parsedout map[string]interface{}
				expectedout := map[string]interface{}{
					"other": 4,
					"nested": map[string]interface{}{
						"key": 9.17,
					},
					"someother": map[string]interface{}{
						"key": map[string]interface{}{
							"something": "goes",
							"here":      true,
						},
					},
					"some": "value",
				}
				er := json.Unmarshal([]byte(in.GetMessage().GetDecoratedPayload()), &parsedout)
				if !reflect.DeepEqual(fmt.Sprintf("%v", parsedout), fmt.Sprintf("%v", expectedout)) {
					t.Logf("mismatch %+v, expected %+v", parsedout, expectedout)
					So(parsedout, ShouldResemble, expectedout)
				}
				// So(parsedout, ShouldResemble, expectedout)
				return er
			})

			xid, er := pipe.Send(&protomessages.MessageEnvelop{
				Payload: "",
				Route:   []string{"two-workers-first", "two-workers-second"},
			})
			So(er, ShouldBeNil)
			So(xid, ShouldNotEqual, "")
			er = pipe.LoadNext()
			So(er, ShouldBeNil)
			worker1.runOne(pipe.CurrentMessage())
			er = pipe2.LoadNext()
			So(er, ShouldBeNil)
			worker2.runOne(pipe2.CurrentMessage())
		})
		Convey("base case, no callbacks", func() {
			worker := New(pipe, "first", 0, false, false)

			Convey("successful message", func() {
				success := false
				worker.OnMessage(func(in *protomessages.Event, w *Worker) error {
					success = true
					return nil
				})
				xid, er := pipe.Send(&protomessages.MessageEnvelop{
					Payload: `{"some":"value"}`,
					Route:   []string{"first"},
				})
				So(er, ShouldBeNil)
				So(xid, ShouldNotEqual, "")
				Convey("with no on success callback", func() {
					er = pipe.LoadNext()
					So(er, ShouldBeNil)
					worker.runOne(pipe.CurrentMessage())
					So(success, ShouldBeTrue)
				})

				Convey("with an on success callback", func() {
					supersuccess := false
					worker.OnSuccess(func(in *protomessages.Event, w *Worker) error {
						supersuccess = true
						return nil
					})
					er = pipe.LoadNext()
					So(er, ShouldBeNil)
					worker.runOne(pipe.CurrentMessage())
					So(supersuccess, ShouldBeTrue)
				})
			})

			Convey("failure message", func() {
				worker.OnMessage(func(in *protomessages.Event, w *Worker) error {
					return errors.New("bad juju")
				})
				xid, er := pipe.Send(&protomessages.MessageEnvelop{
					Payload: `{"some":"value"}`,
					Route:   []string{"first"},
				})
				So(er, ShouldBeNil)
				So(xid, ShouldNotEqual, "")

				Convey("with no on failure callback", func() {
					er = pipe.LoadNext()
					So(er, ShouldBeNil)
					worker.runOne(pipe.CurrentMessage())
				})

				Convey("with an on failure callback", func() {
					superfail := false
					worker.OnFailure(func(in *protomessages.Event, w *Worker) error {
						w.Log(in.GetStringId(), -99, "fleep")
						w.AddStepAfterThis([]string{"badstep"})
						w.Decorate([]*protopipes.Decoration{
							&protopipes.Decoration{
								Key: "some",
								// Value: structpb.NewStringValue("value"),
								Value: `"value"`,
							}})
						superfail = true
						return w.Complete(in.GetStringId())
					})
					er = pipe.LoadNext()
					So(er, ShouldBeNil)
					worker.runOne(pipe.CurrentMessage())
					So(superfail, ShouldBeTrue)
				})
			})
		})
	})
}
