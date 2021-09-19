package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	protopipes "github.com/nochte/pipelinr-protocol/protobuf/pipes"

	protomessages "github.com/nochte/pipelinr-protocol/protobuf/messages"

	"github.com/nochte/pipelinr-clients/go/v2/worker"
)

const WORKERS = 64

func main() {
	file, er := os.Create("./graph.txt")
	if er != nil {
		panic(er)
	}

	out := make(chan string, 1000)

	for i := 0; i < WORKERS; i++ {
		var w *worker.Worker
		if os.Getenv("GRPC") != "" {
			w = worker.NewGRPCWorker(
				os.Getenv("PIPELINR_GRPC_URL"),
				os.Getenv("PIPELINR_API_KEY"),
				"graph")
		} else {
			w = worker.NewHTTPWorker(
				os.Getenv("PIPELINR_URL"),
				os.Getenv("PIPELINR_API_KEY"),
				"graph")
		}
		w.SetReceiveOptions(&protopipes.ReceiveOptions{
			AutoAck:           false,
			Block:             false,
			Count:             5,
			Timeout:           60,
			RedeliveryTimeout: 60 * 10,
		})

		w.OnMessage(func(msg *protomessages.Event) error {
			// log.Printf("got a msg (%v): %v. %v\n", msg.GetStringId(), msg.GetMessage().GetRoute(), msg.GetMessage().GetRouteLog())

			out <- fmt.Sprintf("%v\n", strings.Join(msg.GetMessage().GetRoute(), " -> "))

			return nil
		})
		w.RunNonBlocking()
		time.Sleep(time.Second)
	}

	for line := range out {
		file.WriteString(line)
	}
}
