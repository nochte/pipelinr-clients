package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	protomessages "github.com/nochte/pipelinr-protocol/protobuf/messages"

	pipes "github.com/nochte/pipelinr-clients/go/v2/pipe/http"
	"github.com/nochte/pipelinr-lib/retry"
)

func main() {
	countstr := os.Getenv("COUNT")
	count := -1
	if countstr != "" {
		count, _ = strconv.Atoi(countstr)
	}

	threadstr := os.Getenv("THREADS")
	threads := 1
	if threadstr != "" {
		threads, _ = strconv.Atoi(threadstr)
	}
	url := os.Getenv("PIPELINR_URL")
	apikey := os.Getenv("PIPELINR_API_KEY")
	sender := pipes.New(url, "2", "route", apikey)

	wg := sync.WaitGroup{}
	st := time.Now()
	for j := 0; j < threads; j++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 1; i >= 0; i++ {
				if count > 0 && i > count {
					return
				}
				er := retry.Do(func() error {
					_, er := sender.Send(&protomessages.MessageEnvelop{
						Route:   []string{"route"},
						Payload: fmt.Sprintf(`{"index":%v}`, i),
					})
					return er
				}, 10000, time.Millisecond*50)
				if er != nil {
					panic(fmt.Sprintf("failed to push a message %v\n", er.Error()))
				}
				if i%500 == 0 {
					log.Printf("(%v) (%v) %v messages per sec \n", time.Since(st), i, float64(i)/float64(time.Since(st).Seconds()))
				}
			}
		}()
	}
	wg.Wait()
}
