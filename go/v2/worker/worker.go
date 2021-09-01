package worker

import (
	"errors"
	"fmt"
	"log"
	"time"

	pipes "github.com/nochte/pipelinr-clients/go/v2/pipe"
	httppipes "github.com/nochte/pipelinr-clients/go/v2/pipe/http"
	protomessages "github.com/nochte/pipelinr-protocol/protobuf/messages"
	protopipes "github.com/nochte/pipelinr-protocol/protobuf/pipes"
)

type Worker struct {
	pipe      pipes.Pipe
	step      string
	running   bool
	onMessage []func(*protomessages.Event) error
	onError   []func(*protomessages.Event, error)
}

// NewHTTPWorker spawns a worker that works off of the HTTP protocol
//  if url is left blank, the pipelinr production URL is used
// Default receive options are:
// { AutoAck: false, Block: false, Count: 10, Timeout: 60, RedeliveryTimeout: 120}
func NewHTTPWorker(url, apikey, step string) *Worker {
	if url == "" {
		url = "https://pipelinr.dev"
	}
	p := httppipes.New(url, "2", step, apikey)
	p.SetReceiveOptions(&protopipes.ReceiveOptions{
		AutoAck:           false,
		Block:             false,
		Count:             10,
		Timeout:           60,
		RedeliveryTimeout: 120,
	})
	return &Worker{
		pipe:      p,
		step:      step,
		running:   false,
		onMessage: make([]func(*protomessages.Event) error, 0),
		onError:   make([]func(*protomessages.Event, error), 0),
	}
}

// Pipe returns the worker's pipe
//  useful in cases of OnError, when you want to conditionally 'complete' a message
//  also useful for decorating messages
func (w *Worker) Pipe() pipes.Pipe {
	return w.pipe
}

func (w *Worker) SetReceiveOptions(opts *protopipes.ReceiveOptions) {
	opts.Pipe = w.step
	w.pipe.SetReceiveOptions(opts)
}

// OnMessage takes a func to run on each message
//  if the func returns an error
//   - will log the error to the message
//   - if onErrors
//     - will call all onErrors (but not complete the step)
//   - if no onErrors
//     - will complete the step
//  if the func does not return an error
//   - will complete the step
func (w *Worker) OnMessage(fn func(*protomessages.Event) error) {
	w.onMessage = append(w.onMessage, fn)
}

// OnError: if OnError is provided, then it will be called when an OnMessage fails
//  otherwise, a log will be emitted to the event, and the pipeline will carry on
//  if OnError is provided, then the pipeline will *not* carry on
func (w *Worker) OnError(fn func(*protomessages.Event, error)) {
	w.onError = append(w.onError, fn)
}

func (w *Worker) StopWhenIdle() {
	for !w.pipe.Idle() {
		time.Sleep(time.Second)
	}
	w.Stop()
}

// Stop stops the process
func (w *Worker) Stop() {
	w.running = false
	w.pipe.Stop()
}

// RunNonBlocking starts the process and returns immediately
func (w *Worker) RunNonBlocking() {
	go w.Run()
}

// Run starts the process
func (w *Worker) Run() error {
	if w.running {
		return errors.New("already running")
	}
	w.running = true
	go w.pipe.Start()
	time.Sleep(time.Millisecond * 1000)
	ch := w.pipe.Chan()

	for m := range ch {
		log.Printf("(%v) handling (%v) (%v)\n", w.step, m.GetMessage().Route, m.GetMessage().DecoratedPayload)
		shouldcomplete := true
		keepon := true
		for ndx, fn := range w.onMessage {
			if !keepon {
				continue
			}

			er := fn(m)
			if er != nil {
				keepon = false
				w.pipe.Log(m.GetStringId(), -1, fmt.Sprintf("failed to complete handler %v, with error %v", ndx, er.Error()))
				if len(w.onError) > 0 {
					shouldcomplete = false
				}
				for _, erfn := range w.onError {
					erfn(m, er)
				}
			}
		}
		if shouldcomplete {
			w.pipe.Log(m.GetStringId(), 0, fmt.Sprintf("completed step %v", w.step))
			w.pipe.Complete(m.GetStringId())
		}
	}
	return nil
}
