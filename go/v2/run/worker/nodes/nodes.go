package main

import (
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	protomessages "github.com/nochte/pipelinr-protocol/protobuf/messages"
	protopipes "github.com/nochte/pipelinr-protocol/protobuf/pipes"

	"github.com/nochte/pipelinr-clients/go/v2/worker"
	"github.com/nochte/pipelinr-lib/retry"
)

const WORKERS = 1

func main() {
	whichnodes := strings.Split(os.Getenv("NODES"), ",")
	if len(whichnodes) == 0 || whichnodes[0] == "" {
		whichnodes = []string{
			"node1",
			"node2",
			"node3",
			"node4",
			"node5",
			"node6",
			"node7",
			"node8",
			"node9",
			"node10",
			"node11",
			"node12",
		}
	}
	for i := 0; i < WORKERS; i++ {
		for _, r := range whichnodes {
			log.Printf("sleeping for a bit before starting %v\n", r)
			time.Sleep(time.Millisecond * 250)
			startANode(r)
		}
	}

	done := make(chan bool)
	<-done
}

func startANode(name string) *worker.Worker {
	var w *worker.Worker
	if os.Getenv("GRPC") != "" {
		w = worker.NewGRPCWorker(
			os.Getenv("PIPELINR_GRPC_URL"),
			os.Getenv("PIPELINR_API_KEY"),
			name)
	} else {
		w = worker.NewHTTPWorker(
			os.Getenv("PIPELINR_URL"),
			os.Getenv("PIPELINR_API_KEY"),
			name)
	}
	w.SetReceiveOptions(&protopipes.ReceiveOptions{
		AutoAck:           false,
		Block:             false,
		Count:             5,
		Timeout:           60,
		RedeliveryTimeout: 60 * 10,
	})

	i := 0
	w.OnMessage(func(msg *protomessages.Event) error {
		i++
		retry.Do(func() error {
			return w.Pipe().Log(msg.GetStringId(), 1, "decoration added")
		}, 10, time.Millisecond*100)
		return retry.Do(func() error {
			er := w.Pipe().Decorate(msg.GetStringId(), []*protopipes.Decoration{
				{Key: name, Value: fmt.Sprintf("%v", i)},
			})
			if er != nil {
				log.Printf("DEBUG %v %v\n", name, msg.Id)
				log.Printf("stringid %v\n", msg.GetStringId())
			}
			return er
		}, 10, time.Millisecond*100)
	})
	w.RunNonBlocking()
	return w
}
