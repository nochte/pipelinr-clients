package pipes

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/nochte/pipelinr-protocol/protobuf/accounts"
	protomessages "github.com/nochte/pipelinr-protocol/protobuf/messages"
	protopipes "github.com/nochte/pipelinr-protocol/protobuf/pipes"
	"github.com/nochte/pipelinr/entrypoints/http/service"
	repoinmemory "github.com/nochte/pipelinr/repositories/inmemory"
	repomongo "github.com/nochte/pipelinr/repositories/mongo"
	"github.com/nochte/pipelinr/services"
	. "github.com/smartystreets/goconvey/convey"
)

func TestPipe_Send(t *testing.T) {
	if os.Getenv("NO_HTTP") != "" {
		return
	}

	Convey("http pipe interaction", t, func() {
		port := fmt.Sprintf(":%v", rand.Intn(1000)+9000)
		// turn on an http server
		accounting := repoinmemory.DefaultInMemoryEventAccountingRepository()
		repo := repoinmemory.DefaultInMemoryEventRepository()
		redelivery := repoinmemory.NewRedeliveryCache()
		pipesvc := services.NewPipeService(repo, accounting, redelivery, os.Getenv("RABBITMQ_URL"))
		storesvc := services.NewStoreService(repo)
		healthsvc := services.NewHealthcheckService(pipesvc, storesvc)

		users := repomongo.DefaultUserRepository("pipelinr", "testpipe-users")
		apikeys := repomongo.DefaultAPIKeyRepository("pipelinr", "testpipe-apikeys")
		apikeys.Create(&accounts.APIKey{
			UserId:          "testpipe",
			Key:             "foo",
			ActivationState: accounts.UserActivationState_Active,
		})
		opts, er := redis.ParseURL(os.Getenv("REDIS_URL"))
		if er != nil {
			t.Fatalf("couldn't parse redis url %v", er)
		}
		rediser := redis.NewClient(opts)
		accountssvc := services.NewAccountsService(users, apikeys, rediser)

		go service.NewHTTPService(pipesvc, storesvc, healthsvc, accountssvc, port).Run()

		time.Sleep(time.Second * 5)

		pipefer := func(name string) *Pipe {
			p := New(
				fmt.Sprintf("http://localhost%v", port),
				"2",
				name,
				"foo",
			)
			p.SetReceiveOptions(&protopipes.ReceiveOptions{
				AutoAck: true,
				Timeout: 10,
				Block:   true,
				// Count:             100,
				Count:             1,
				Pipe:              name,
				RedeliveryTimeout: 5,
			})
			return p
		}

		Convey("send with bad info", func() {
			id, er := pipefer("send-with-bad-info").Send(&protomessages.MessageEnvelop{
				Payload: `{"hello":"world"}`,
			})
			So(er, ShouldNotBeNil)
			So(id, ShouldEqual, "")
		})

		Convey("send with good info", func() {
			id, er := pipefer("send-with-good-info").Send(&protomessages.MessageEnvelop{
				Route:   []string{"send-with-good-info", "second"},
				Payload: `{"hello":"world"}`,
			})
			id2, er2 := pipefer("send-with-good-info").Send(&protomessages.MessageEnvelop{
				Route:   []string{"send-with-good-info"},
				Payload: `{"hello":"world"}`,
			})
			So(er, ShouldBeNil)
			So(id, ShouldNotEqual, "")
			So(er2, ShouldBeNil)
			So(id2, ShouldNotEqual, "")

			Convey("LoadNext", func() {
				pf := pipefer("send-with-good-info")
				er := pf.LoadNext()
				So(er, ShouldBeNil)
				So(pf.CurrentMessage(), ShouldNotBeNil)
				So(pf.CurrentMessage().GetStringId(), ShouldEqual, id)

				Convey("Ack", func() {
					So(pf.Ack(id), ShouldBeNil)
					So(pf.Log(id, 12, "sometext"), ShouldBeNil)
					So(pf.Decorate(id, []*protopipes.Decoration{
						&protopipes.Decoration{Key: "some", Value: "value"},
						&protopipes.Decoration{Key: "other", Value: "value2"},
					}), ShouldBeNil)
					So(pf.AddSteps(id, []string{"fleep", "floop"}), ShouldBeNil)
					So(pf.Complete(id), ShouldBeNil)

					pf2 := pipefer("second")
					So(pf2.LoadNext(), ShouldBeNil)
					So(pf2.CurrentMessage().GetStringId(), ShouldEqual, id)
					msrexpected := map[string]interface{}{
						"hello": "world",
						"some":  "value",
						"other": "value2",
					}
					msractual := map[string]interface{}{}
					json.Unmarshal([]byte(pf2.CurrentMessage().GetMessage().GetDecoratedPayload()), &msractual)
					So(msrexpected, ShouldResemble, msractual)
					So(pf2.CurrentMessage().GetMessage().GetRoute(), ShouldEqual, []string{
						"send-with-good-info", "second", "fleep", "floop",
					})

					So(pf.LoadNext(), ShouldBeNil)
					So(pf.CurrentMessage(), ShouldNotBeNil)
					So(pf.CurrentMessage().GetStringId(), ShouldEqual, id2)
				})
			})
		})
	})
	// type args struct {
	// 	msg *protomessages.MessageEnvelop
	// }
	// tests := []struct {
	// 	name    string
	// 	url     string
	// 	args    args
	// 	want    string
	// 	wantErr bool
	// }{
	// 	{name: "with a good message", url: "http://localhost:8090", args: args{msg: &protomessages.MessageEnvelop{
	// 		Route:   []string{"fleep"},
	// 		Payload: `{"hello":"world}"`,
	// 	}}, want: "something", wantErr: false},
	// 	{name: "with a bad message", url: "http://localhost:8090", args: args{msg: &protomessages.MessageEnvelop{
	// 		Payload: `{"hello":"world}"`,
	// 	}}, want: "", wantErr: true},
	// 	{name: "with a bad url", url: "http://localhost:9090", args: args{msg: &protomessages.MessageEnvelop{
	// 		Payload: `{"hello":"world}"`,
	// 	}}, want: "", wantErr: true},
	// }
	// for _, tt := range tests {
	// 	t.Run(tt.name, func(t *testing.T) {
	// 		p := &Pipe{
	// 			urlbase: tt.url,
	// 			version: "2",
	// 		}
	// 		got, err := p.Send(tt.args.msg)
	// 		if (err != nil) != tt.wantErr {
	// 			t.Errorf("Pipe.Send() error = %v, wantErr %v", err, tt.wantErr)
	// 			return
	// 		}
	// 		if tt.want != "" && got == "" {
	// 			t.Errorf("Pipe.Send() %v, wanted something", got)
	// 		}
	// 	})
	// }
}
