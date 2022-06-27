package worker

import (
	"errors"
	"fmt"

	"github.com/nochte/pipelinr-clients/go/lib/retry"
	"github.com/nochte/pipelinr-clients/go/pipe"
	"github.com/nochte/pipelinr-protocol/protobuf/messages"
)

type Worker struct {
	pipe      *pipe.Pipe
	onMessage []func(*messages.Event, *pipe.Pipe) error
	onError   []func(*messages.Event, error, *pipe.Pipe)
	running   bool
}

func New(p *pipe.Pipe) *Worker {
	return &Worker{
		pipe:      p,
		onMessage: make([]func(*messages.Event, *pipe.Pipe) error, 0, 1),
		onError:   make([]func(*messages.Event, error, *pipe.Pipe), 0, 1),
		running:   false}
}

func NewHTTP(step, apikey, url string) *Worker {
	return New(pipe.NewHTTP(url, apikey, step))
}

func NewGRPC(step, apikey, url string) *Worker {
	return New(pipe.NewGRPC(url, apikey, step))
}

func (w Worker) Pipe() *pipe.Pipe {
	return w.pipe
}

func (w Worker) Name() string {
	return w.pipe.Name()
}

func (w Worker) Step() string {
	return w.pipe.Name()
}

func (w Worker) SetReceiveOptions(count *int32, timeout *int64, redeliveryTimeout *int64, autoAck, block, excludeRouting, excludeRouteLog, excludeDecoratedPayload *bool) {
	w.pipe.SetReceiveOptions(count, timeout, redeliveryTimeout, autoAck, block, excludeRouting, excludeRouteLog, excludeDecoratedPayload)
}

func (w *Worker) OnMessage(in func(*messages.Event, *pipe.Pipe) error) {
	w.onMessage = append(w.onMessage, in)
}

func (w *Worker) OnError(in func(*messages.Event, error, *pipe.Pipe)) {
	w.onError = append(w.onError, in)
}

func (w *Worker) Stop() {
	w.running = false
	w.pipe.Stop()
}

func (w *Worker) Run() error {
	if w.running {
		return errors.New("already running")
	}
	w.running = true
	go func() {
		if er := w.pipe.Start(0); er != nil {
			w.Stop()
		}
	}()

	ch := w.pipe.Chan()

	for w.running {
		msg := <-ch

		shouldcomplete := true
		keepon := true

		for ndx := range w.onMessage {
			if !keepon {
				continue
			}

			if er := w.onMessage[ndx](msg, w.pipe); er != nil {
				keepon = false
				retry.Do(func() error {
					return w.pipe.Log(msg.GetStringId(), -1, fmt.Sprintf("failed to run handler %v, with error %v", ndx, er.Error()))
				}, 40, 250)

				if len(w.onError) > 0 {
					shouldcomplete = false
				}

				for ndy := range w.onError {
					w.onError[ndy](msg, er, w.pipe)
				}
			}

			if shouldcomplete {
				retry.Do(func() error {
					return w.pipe.Log(msg.GetStringId(), 0, fmt.Sprintf("completed step %v", w.Step()))
				}, 40, 250)
				retry.Do(func() error {
					return w.pipe.Complete(msg.GetStringId())
				}, 40, 250)
			}
		}
	}

	return errors.New("worker stopped")
}
