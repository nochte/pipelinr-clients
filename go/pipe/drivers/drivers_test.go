package drivers

import (
	"encoding/json"
	"testing"

	"github.com/nochte/pipelinr-clients/go/lib"
	"github.com/nochte/pipelinr-protocol/protobuf/pipes"
	. "github.com/smartystreets/goconvey/convey"
)

func TestDriver(t *testing.T) {
	Convey("Driver tests", t, func() {
		type TestDefinition struct {
			getdriver func() Driver
			name      string
		}
		for _, td := range []TestDefinition{
			{name: "grpc driver", getdriver: func() Driver {
				return NewGRPCDriver("", "")
			}},
			{name: "http driver", getdriver: func() Driver {
				return NewHTTPDriver("", "")
			}},
		} {
			Convey(td.name, func() {
				driver := td.getdriver()

				Convey("send should return an id", func() {
					res, er := driver.Send(`{"foo":"bar"}`, []string{"blackhole"})
					So(er, ShouldBeNil)
					So(len(res), ShouldNotEqual, 0)
				})

				Convey("message sent", func() {
					route := []string{lib.GenerateRandomString(8), lib.GenerateRandomString(8)}
					mid, er := driver.Send(`{"foo":"bar"}`, route)
					So(er, ShouldBeNil)

					Reset(func() {
						for _, elm := range route {
							driver.Complete(mid, elm)
						}
					})

					Convey("recv should return an event in the proper format", func() {
						evts, er := driver.Recv(&pipes.ReceiveOptions{
							Pipe:    route[0],
							Count:   1,
							Block:   true,
							AutoAck: true,
						})
						So(er, ShouldBeNil)
						So(len(evts), ShouldEqual, 1)
						So(evts[0].GetMessage().GetPayload(), ShouldEqual, `{"foo":"bar"}`)
						So(evts[0].GetMessage().GetDecoratedPayload(), ShouldEqual, `{"foo":"bar"}`)
						So(evts[0].GetMessage().GetRoute(), ShouldResemble, route)
					})

					Convey("message has been received", func() {
						evts, er := driver.Recv(&pipes.ReceiveOptions{
							Pipe:    route[0],
							Count:   1,
							Block:   true,
							AutoAck: true,
						})
						So(er, ShouldBeNil)
						So(len(evts), ShouldEqual, 1)
						event := evts[0]

						Convey("ack", func() {
							So(driver.Ack(event.GetStringId(), route[0]), ShouldBeNil)
							So(driver.Ack(event.GetStringId(), route[0]), ShouldBeNil)
							So(driver.Ack("badid", route[0]), ShouldBeNil)
						})

						Convey("complete", func() {
							So(driver.Complete(event.GetStringId(), route[0]), ShouldBeNil)
							So(driver.Complete(event.GetStringId(), route[0]), ShouldNotBeNil)
							So(driver.Complete("badid", route[0]), ShouldNotBeNil)
						})

						Convey("logging", func() {
							So(driver.AppendLog(event.GetStringId(), route[0], 33, "some message here"), ShouldBeNil)
							driver.Complete(event.GetStringId(), route[0])
							evts, er := driver.Recv(&pipes.ReceiveOptions{
								Pipe:    route[1],
								Count:   1,
								Block:   true,
								AutoAck: true})

							So(er, ShouldBeNil)
							So(len(evts), ShouldEqual, 1)
							msg := evts[0]
							So(len(msg.GetMessage().GetRouteLog()), ShouldEqual, 1)
							So(msg.GetMessage().GetRouteLog()[0].GetStep(), ShouldEqual, route[0])
							So(msg.GetMessage().GetRouteLog()[0].GetCode(), ShouldEqual, 33)
							So(msg.GetMessage().GetRouteLog()[0].GetMessage(), ShouldEqual, "some message here")
						})

						Convey("adding steps", func() {
							So(driver.AddStepsAfter(event.GetStringId(), route[0], []string{"added", "after"}), ShouldBeNil)
							driver.Complete(event.GetStringId(), route[0])
							evts, er := driver.Recv(&pipes.ReceiveOptions{
								Pipe:    "added",
								Count:   1,
								Block:   true,
								AutoAck: true})

							So(er, ShouldBeNil)
							So(len(evts), ShouldEqual, 1)
							msg := evts[0]

							So(msg.GetMessage().GetRoute()[2], ShouldEqual, "after")
							So(msg.GetMessage().GetRoute()[3], ShouldEqual, route[1])
							driver.Complete(event.GetStringId(), "added")
							driver.Complete(event.GetStringId(), "after")
						})

						Convey("decorations", func() {
							ers := driver.Decorate(event.GetStringId(), []*pipes.Decoration{
								{Key: "foo", Value: "bar"},
								{Key: "flip", Value: "fleeeep"}})
							So(len(ers), ShouldEqual, 2)
							So(ers[0], ShouldBeNil)
							So(ers[1], ShouldBeNil)

							driver.Complete(event.GetStringId(), route[0])
							evts, er := driver.Recv(&pipes.ReceiveOptions{
								Pipe:    route[1],
								Count:   1,
								Block:   true,
								AutoAck: true})

							So(er, ShouldBeNil)
							So(len(evts), ShouldEqual, 1)
							msg := evts[0]
							var res map[string]interface{}
							So(json.Unmarshal([]byte(msg.GetMessage().GetDecoratedPayload()), &res), ShouldBeNil)
							So(res["foo"], ShouldEqual, "bar")
							So(res["flip"], ShouldEqual, "fleeeep")

							decs, er := driver.GetDecorations(event.GetStringId(), []string{"foo", "bar", "flip"})
							So(er, ShouldBeNil)
							So(len(decs), ShouldEqual, 3)
							So(decs[0].GetValue(), ShouldEqual, `"bar"`)
							So(decs[1], ShouldBeNil)
							So(decs[2].GetValue(), ShouldEqual, `"fleeeep"`)
						})
					})
				})
			})
		}
	})
}
