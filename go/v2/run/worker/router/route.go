package main

import (
	"math/rand"
	"os"
	"time"

	protomessages "github.com/nochte/pipelinr-protocol/protobuf/messages"

	worker "github.com/nochte/pipelinr-clients/go/v2/worker"
)

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

	for i := 0; i < 4; i++ {
		router := worker.NewHTTPWorker(
			os.Getenv("PIPELINR_URL"),
			os.Getenv("PIPELINR_API_KEY"),
			"route")

		router.OnMessage(func(msg *protomessages.Event) error {
			// log.Printf("handling %v\n", msg.GetStringId())
			newroute := make([]string, 0)
			for _, r := range ROUTES {
				if rand.Int()%2 == 0 {
					newroute = append(newroute, r)
				}
			}
			newroute = append(newroute, "graph")
			er := router.Pipe().AddSteps(msg.GetStringId(), newroute)
			time.Sleep(time.Millisecond * 10)
			return er
		})
		go router.Run()
	}
	<-make(chan bool)
}
