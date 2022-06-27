package retry

import (
	"log"
	"os"
	"runtime"
	"time"
)

// Do tries something <retrycount> times, with <retrybackoff * num retries> between each attempt
//  on error. if all retries fail, then it returns the last error
func Do(fn func() error, retrycount int, retrybackoff time.Duration) error {
	var er error
	for i := 1; i <= retrycount; i++ {

		er = fn()
		if er == nil {
			return nil
		}
		_, file, line, _ := runtime.Caller(1)
		if os.Getenv("PIPELINR_DEBUG") != "" {
			log.Printf("RETRY FAILURE: (file %v) (line %v): %v\n", file, line, er.Error())
		}

		time.Sleep(retrybackoff * time.Duration(i))
	}
	return er
}
