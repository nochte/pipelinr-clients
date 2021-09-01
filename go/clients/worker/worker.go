package worker

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	protomessages "github.com/nochte/pipelinr-protocol/protobuf/messages"
	protopipes "github.com/nochte/pipelinr-protocol/protobuf/pipes"
	"github.com/nochte/pipelinr/clients/pipes"
	"github.com/nochte/pipelinr/telemetry"
)

// Worker implements the worker interaction on top of a client pipe
type Worker struct {
	pipe            pipes.Pipe
	name            string
	workfunc        func(*protomessages.Event, *Worker) error
	onfailfunc      func(*protomessages.Event, *Worker) error
	onsuccessfunc   func(*protomessages.Event, *Worker) error
	ackonerror      bool
	completeonerror bool
	running         bool
}

// New instantiates one
//  if ackonerror = true, will send ack even if error is thrown
//  if completeonerror = true, will complete even if error is thrown
//  if pipeline to continue after error, OnFailure MUST be given, and MUST call Complete manually
func New(pipe pipes.Pipe, name string, redeliverytimeout int64, ackonerror, completeonerror bool) *Worker {
	if redeliverytimeout == 0 {
		redeliverytimeout = 30
	}
	pipe.SetReceiveOptions(&protopipes.ReceiveOptions{
		AutoAck: false,
		Block:   true,
		Count:   5,
		// Count:             1,
		Pipe:              name,
		RedeliveryTimeout: redeliverytimeout,
	})
	return &Worker{
		pipe:            pipe,
		name:            name,
		ackonerror:      ackonerror,
		completeonerror: completeonerror,
	}
}

// to be deferred
func (w *Worker) ackWithError(id string) {
	if er := w.pipe.Ack(id); er != nil {
		log.Printf("error acking: %v\n", er)
	}
}

// to be deferred
func (w *Worker) completeWithError(id string) {
	if er := w.pipe.Complete(id); er != nil {
		log.Printf("error completing: %v (%v)\n", er, id)
	}
}

func (w *Worker) runOne(in *protomessages.Event) {
	defer telemetry.TimeAndCount(telemetry.TimerStart(), "worker", "runone", w.name)
	st := time.Now().UnixNano() / 1000000.0
	completemsg := "succeeded"
	code := 0
	if er := w.workfunc(in, w); er != nil {
		// if onfailfunc, call it
		completemsg = fmt.Sprintf("failed (error %v)", er)
		code = -1
		if w.onfailfunc != nil {
			if er = w.onfailfunc(in, w); er != nil {
				log.Printf("error running onfailfunc: %v\n", er)
			}
		}
		if w.completeonerror {
			defer w.completeWithError(in.GetStringId())
		} else if w.ackonerror {
			defer w.ackWithError(in.GetStringId())
		}
	} else {
		// if onsuccessfunc, call it
		if w.onsuccessfunc != nil {
			if er = w.onsuccessfunc(in, w); er != nil {
				log.Printf("error running onsuccessfunc: %v\n", er)
			}
		}
		defer w.completeWithError(in.GetStringId())
	}
	elapsedms := (time.Now().UnixNano() / 1000000.0) - st
	jsmsg, _ := json.Marshal(map[string]interface{}{
		"elapsedms": elapsedms,
		"message":   completemsg,
	})
	if er := w.Log(in.GetStringId(), int32(code), string(jsmsg)); er != nil {
		log.Printf("error adding log: %v (%v / %v)\n", er, in.GetStringId(), w.pipe.CurrentMessage().GetStringId())
	}
}

func (w *Worker) Run() error {
	if w.running {
		return errors.New("already running")
	}
	w.running = true
	for {
		if w.pipe.CurrentMessage() == nil {
			er := w.pipe.LoadNext()
			if er != nil {
				log.Printf("error getting next message. trying again in a few seconds. %v\n", er)
				time.Sleep(time.Second * 5)
				continue
			}

			w.runOne(w.pipe.CurrentMessage())
		}
	}
}

func (w *Worker) OnMessage(callback func(*protomessages.Event, *Worker) error) {
	w.workfunc = callback
}

func (w *Worker) OnSuccess(callback func(*protomessages.Event, *Worker) error) {
	w.onsuccessfunc = callback
}
func (w *Worker) OnFailure(callback func(*protomessages.Event, *Worker) error) {
	w.onfailfunc = callback
}

// func (w *Worker) Ack() error {
// 	return w.pipe.Ack()
// }

func (w *Worker) Complete(id string) error {
	defer telemetry.TimeAndCount(telemetry.TimerStart(), "worker", "complete", w.name)
	return w.pipe.Complete(id)
}

func (w *Worker) Log(id string, code int32, text string) error {
	defer telemetry.TimeAndCount(telemetry.TimerStart(), "worker", "log", w.name)
	return w.pipe.Log(id, code, text)
}

func (w *Worker) AddStepAfterThis(in []string) error {
	defer telemetry.TimeAndCount(telemetry.TimerStart(), "worker", "addstepsafterthis", w.name)

	return w.pipe.AddSteps(w.pipe.CurrentMessage().GetStringId(), in)
}

func (w *Worker) Decorate(in []*protopipes.Decoration) error {
	defer telemetry.TimeAndCount(telemetry.TimerStart(), "worker", "decorate", w.name)

	return w.pipe.Decorate(w.pipe.CurrentMessage().GetStringId(), in)
}
