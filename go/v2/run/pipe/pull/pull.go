package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	protopipes "github.com/nochte/pipelinr-protocol/protobuf/pipes"

	pipes "github.com/nochte/pipelinr-clients/go/v2/pipe/http"
)

func main() {
	step := os.Getenv("STEP")
	threadstr := os.Getenv("THREADS")
	threads := 1
	if threadstr != "" {
		threads, _ = strconv.Atoi(threadstr)
	}
	url := os.Getenv("PIPELINR_URL")
	apikey := os.Getenv("PIPELINR_API_KEY")

	st := time.Now()
	for j := 0; j < threads; j++ {
		go func(workernum int) {
			receiver := pipes.New(url, "2", step, apikey)
			receiver.SetReceiveOptions(&protopipes.ReceiveOptions{
				AutoAck:           false,
				Block:             true,
				Count:             10,
				Timeout:           10,
				Pipe:              step,
				RedeliveryTimeout: 300,
			})
			go func() {
				if er := receiver.Start(); er != nil {
					panic(fmt.Sprintf("failed to start receiver %v\n", er.Error()))
				}
			}()

			time.Sleep(time.Second)

			ch := receiver.Chan()

			i := 0
			for m := range ch {
				if er := receiver.Log(m.GetStringId(), int32(i), fmt.Sprintf("finished iteration %v", i)); er != nil {
					log.Printf("error logging %v\n", er.Error())
				}
				if er := receiver.Decorate(m.GetStringId(), []*protopipes.Decoration{
					{Key: fmt.Sprintf("worker-%v", workernum), Value: fmt.Sprintf("%v", i)},
				}); er != nil {
					log.Printf("error logging %v\n", er.Error())
				}
				// if er := receiver.Ack(m.GetStringId()); er != nil {
				// 	log.Printf("error acking %v\n", er.Error())
				// }
				time.Sleep(time.Millisecond * 100)
				if er := receiver.Complete(m.GetStringId()); er != nil {
					log.Printf("error completing %v\n", er.Error())
				}

				i++
				if i%1000 == 0 {
					log.Printf("(%v) (%v) %v messages per sec \n", time.Since(st), i, float64(i)/float64(time.Since(st).Seconds()))
				}

			}
		}(j)
	}
	wg := sync.WaitGroup{}
	wg.Add(1)
	wg.Wait()
}
