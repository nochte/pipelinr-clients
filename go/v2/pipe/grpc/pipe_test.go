package pipes

import (
	"fmt"
	"log"
	"os"
	"sync"
	"testing"
	"time"

	protomessages "github.com/nochte/pipelinr-protocol/protobuf/messages"
	protopipes "github.com/nochte/pipelinr-protocol/protobuf/pipes"

	. "github.com/smartystreets/goconvey/convey"
)

func TestPipe(t *testing.T) {
	url := os.Getenv("PIPELINR_GRPC_URL")
	apikey := os.Getenv("PIPELINR_API_KEY")

	Convey("pipe flow", t, func() {
		ps1 := New(url, "ps1", apikey)

		id, er := ps1.Send(&protomessages.MessageEnvelop{
			Route:   []string{"ps2", "ps3"},
			Payload: `{"some":"payload"}`,
		})

		So(er, ShouldBeNil)
		So(id, ShouldNotEqual, "")

		ps2 := New(url, "ps2", apikey)
		ps3 := New(url, "ps3", apikey)

		go func() {
			if er := ps2.Start(); er != nil {
				panic("couldn't start ps2")
			}
		}()
		go func() {
			if er := ps3.Start(); er != nil {
				panic("couldn't start ps3")
			}
		}()

		t.Log("pausing a sec to let the chans come up")

		time.Sleep(time.Second)

		ps2chan := ps2.Chan()
		ps3chan := ps3.Chan()

		if msg := <-ps2chan; msg != nil {
			So(ps2.Ack(msg.GetStringId()), ShouldBeNil)
			So(ps2.Log(msg.GetStringId(), -1, "some message"), ShouldBeNil)
			log.Printf("msg %v\n", msg)
			So(ps2.Decorate(msg.GetStringId(), []*protopipes.Decoration{
				{Key: "some", Value: "value"},
				{Key: "other", Value: "value"}}), ShouldBeNil)
			So(ps2.Complete(msg.GetStringId()), ShouldBeNil)
		} else {
			t.Error("error getting a message from ps2")
		}
		if msg := <-ps3chan; msg != nil {
			So(len(msg.Message.GetRouteLog()), ShouldEqual, 1)
			So(msg.GetMessage().GetDecoratedPayload(), ShouldEqual, `{"other":"value","some":"value"}`)
			So(ps3.Complete(msg.GetStringId()), ShouldBeNil)
		} else {
			t.Error("error getting a message from ps3")
		}
	})
}

const NUM_PUSHERS = 50
const NUM_MESSAGES = 100

func TestPipeALot(t *testing.T) {
	url := os.Getenv("PIPELINR_URL")
	apikey := os.Getenv("PIPELINR_API_KEY")

	Convey("pipe load flow", t, func() {
		wg := sync.WaitGroup{}
		for j := 0; j < NUM_PUSHERS; j++ {
			wg.Add(1)
			go func() {
				sender := New(url, "load-1", apikey)
				for i := 0; i < NUM_MESSAGES; i++ {
					wg.Add(1)
					func(i int) {
						_, er := sender.Send(&protomessages.MessageEnvelop{
							Route:   []string{"load-1", "load-2", "load-3", "load-4", "load-5"},
							Payload: fmt.Sprintf(`{"index":%v"}`, i),
						})
						if er != nil {
							panic(fmt.Sprintf("error sending %v\n", er))
						}
						wg.Done()
					}(i)
				}
				wg.Done()
			}()
		}

		// wg.Wait()

		wg.Add(5)

		go func() {
			ps1 := New(url, "load-1", apikey)
			go ps1.Start()
			ps1chan := ps1.Chan()
			for i := 0; i < NUM_MESSAGES*NUM_PUSHERS; i++ {
				log.Printf("getting %v \n", i)
				if msg := <-ps1chan; msg != nil {
					So(ps1.Ack(msg.GetStringId()), ShouldBeNil)
					ps1.Complete(msg.GetStringId())
				} else {
					panic("error getting a message from ps1")
				}
			}
			wg.Done()
		}()
		go func() {
			ps2 := New(url, "load-2", apikey)
			go ps2.Start()
			if er := ps2.Start(); er != nil {
				panic("couldn't start ps2")
			}
			ps2chan := ps2.Chan()
			for i := 0; i < NUM_MESSAGES*NUM_PUSHERS; i++ {
				if msg := <-ps2chan; msg != nil {
					So(ps2.Ack(msg.GetStringId()), ShouldBeNil)
					ps2.Complete(msg.GetStringId())
				} else {
					panic("error getting a message from ps1")
				}
			}
			wg.Done()
		}()
		go func() {
			ps3 := New(url, "load-3", apikey)
			go ps3.Start()
			if er := ps3.Start(); er != nil {
				panic("couldn't start ps3")
			}
			ps3chan := ps3.Chan()
			for i := 0; i < NUM_MESSAGES*NUM_PUSHERS; i++ {
				if msg := <-ps3chan; msg != nil {
					So(ps3.Ack(msg.GetStringId()), ShouldBeNil)
					ps3.Complete(msg.GetStringId())
				} else {
					panic("error getting a message from ps1")
				}
			}
			wg.Done()
		}()
		go func() {
			ps4 := New(url, "load-4", apikey)
			go ps4.Start()
			if er := ps4.Start(); er != nil {
				panic("couldn't start ps4")
			}
			ps4chan := ps4.Chan()
			for i := 0; i < NUM_MESSAGES*NUM_PUSHERS; i++ {
				if msg := <-ps4chan; msg != nil {
					So(ps4.Ack(msg.GetStringId()), ShouldBeNil)
					ps4.Complete(msg.GetStringId())
				} else {
					panic("error getting a message from ps1")
				}
			}
			wg.Done()
		}()
		go func() {
			ps5 := New(url, "load-5", apikey)
			go ps5.Start()
			if er := ps5.Start(); er != nil {
				panic("couldn't start ps5")
			}
			ps5chan := ps5.Chan()
			for i := 0; i < NUM_MESSAGES*NUM_PUSHERS; i++ {
				if msg := <-ps5chan; msg != nil {
					So(ps5.Ack(msg.GetStringId()), ShouldBeNil)
					ps5.Complete(msg.GetStringId())
				} else {
					panic("error getting a message from ps1")
				}
			}
			wg.Done()
		}()

		wg.Wait()
	})
}
