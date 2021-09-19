package main

import (
	"math/rand"
	"os"
	"time"

	protomessages "github.com/nochte/pipelinr-protocol/protobuf/messages"
	protopipes "github.com/nochte/pipelinr-protocol/protobuf/pipes"

	worker "github.com/nochte/pipelinr-clients/go/v2/worker"
)

const WORKERS = 64

func main() {
	ROUTES := []string{
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

	for i := 0; i < WORKERS; i++ {
		var router *worker.Worker
		if os.Getenv("GRPC") != "" {
			router = worker.NewGRPCWorker(
				os.Getenv("PIPELINR_GRPC_URL"),
				os.Getenv("PIPELINR_API_KEY"),
				"route")
		} else {
			router = worker.NewHTTPWorker(
				os.Getenv("PIPELINR_URL"),
				os.Getenv("PIPELINR_API_KEY"),
				"route")
		}

		router.SetReceiveOptions(&protopipes.ReceiveOptions{
			AutoAck:           false,
			Block:             false,
			Count:             5,
			Timeout:           60,
			RedeliveryTimeout: 60 * 10,
		})

		rand.Seed(time.Now().UnixNano())

		router.OnMessage(func(msg *protomessages.Event) error {
			// log.Printf("handling %v\n", msg.GetStringId())
			newroute := make([]string, 0)
			for _, r := range ROUTES {
				if rand.Int()%2 == 0 {
					newroute = append(newroute, r)
				}
			}
			rand.Shuffle(len(newroute), func(i, j int) { newroute[i], newroute[j] = newroute[j], newroute[i] })
			newroute = append(newroute, "graph")
			er := router.Pipe().AddSteps(msg.GetStringId(), newroute)
			time.Sleep(time.Millisecond * 10)
			return er
		})
		go router.Run()
	}
	<-make(chan bool)
}
