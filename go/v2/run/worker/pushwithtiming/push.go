package main

import (
	"container/ring"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	pipes "github.com/nochte/pipelinr-clients/go/v2/pipe"

	"github.com/nochte/pipelinr-lib/retry"
	protomessages "github.com/nochte/pipelinr-protocol/protobuf/messages"

	grpcpipes "github.com/nochte/pipelinr-clients/go/v2/pipe/grpc"
	httppipes "github.com/nochte/pipelinr-clients/go/v2/pipe/http"
)

func main() {
	url := os.Getenv("PIPELINR_URL")
	apikey := os.Getenv("PIPELINR_API_KEY")

	persecstr := os.Getenv("PER_SECOND")
	persec := 1
	if persecstr != "" {
		persec, _ = strconv.Atoi(persecstr)
	}

	countstr := os.Getenv("COUNT")
	count := -1
	if countstr != "" {
		count, _ = strconv.Atoi(countstr)
	}

	wg := sync.WaitGroup{}

	numpipes := 500
	outp := ring.New(numpipes)
	for i := 0; i < numpipes; i++ {
		if os.Getenv("GRPC") != "" {
			outp.Value = grpcpipes.New(os.Getenv("PIPELINR_GRPC_URL"), "route", apikey)
		} else {
			outp.Value = httppipes.New(url, "2", "route", apikey)
		}

		outp = outp.Next()
	}

	messages := make(chan *protomessages.MessageEnvelop, 10000)
	sentchan := make(chan bool, 50)

	go func() {
		count := 1
		st := time.Now()
		for range sentchan {
			count++
			if count%500 == 0 {
				log.Printf("(%v) (%v) %v messages per sec \n", time.Since(st), count, float64(count)/float64(time.Since(st).Seconds()))
			}
		}
	}()

	go func() {
		for message := range messages {
			wg.Add(1)
			go func(m *protomessages.MessageEnvelop) {
				retry.Do(func() error {
					_, er := outp.Value.(pipes.Pipe).Send(m)
					outp = outp.Next()
					return er
				}, 100, time.Millisecond*50)
				sentchan <- true
				defer wg.Done()
			}(message)
		}
	}()

	dur := time.Duration(float64(time.Second) / float64(persec))
	for i := 0; count < 1 || i < count; i++ {
		time.Sleep(dur)
		messages <- &protomessages.MessageEnvelop{
			Route:   []string{"route"},
			Payload: fmt.Sprintf(`{"index":%v}`, i+persec),
		}
	}

	close(messages)
	wg.Wait()
	close(sentchan)
}
