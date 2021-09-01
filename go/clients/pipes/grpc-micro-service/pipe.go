package pipes

import (
	"context"
	"errors"
	"log"
	"os"
	"time"

	"github.com/micro/go-micro/v2/registry"
	"github.com/micro/go-micro/v2/registry/etcd"
	"github.com/micro/go-micro/v2/service"
	"github.com/micro/go-micro/v2/service/grpc"
	protomessages "github.com/nochte/pipelinr-protocol/protobuf/messages"
	protopipes "github.com/nochte/pipelinr-protocol/protobuf/pipes"
	"github.com/nochte/pipelinr/telemetry"
)

type Pipe struct {
	receiveoptions *protopipes.ReceiveOptions
	logstep        string
	pipeservice    protopipes.PipeService

	currentmessage   *protomessages.Event
	bufferedmessages []*protomessages.Event
}

func New(endpoint string, recvopts *protopipes.ReceiveOptions) *Pipe {
	opts := []service.Option{
		service.Name(endpoint + ".client"),
		service.RegisterTTL(time.Second * 10),
		service.RegisterInterval(time.Second * 3),
		service.Registry(etcd.NewRegistry(
			registry.Addrs(os.Getenv("MICRO_REGISTRY_ADDRESS")),
		)),
	}
	svr := grpc.NewService(
		opts...,
	)

	pipesvc := protopipes.NewPipeService(endpoint, svr.Client())

	p := &Pipe{
		logstep:     "unknown",
		pipeservice: pipesvc,
	}
	p.SetReceiveOptions(recvopts)
	return p
}

func (p *Pipe) SetReceiveOptions(recvopts *protopipes.ReceiveOptions) {
	p.receiveoptions = recvopts
	if p.receiveoptions.GetPipe() != "" {
		p.logstep = p.receiveoptions.GetPipe()
	}
	p.receiveoptions.Count = 1
}

func (p *Pipe) Send(msg *protomessages.MessageEnvelop) (string, error) {
	defer telemetry.TimeAndCount(telemetry.TimerStart(), "pipe", "grpc", "send", p.logstep)
	out, er := p.pipeservice.Send(context.Background(), msg)
	return out.GetXId(), er
}

const initialWait = time.Millisecond
const waitStep = time.Millisecond * 100
const maximumWait = time.Second * 10

func (p *Pipe) getWithBackoff() *protomessages.Events {
	backoff := int64(initialWait)
	cycle := int64(0)
	for {
		evts, er := p.pipeservice.Recv(context.Background(), p.receiveoptions)
		// if er != nil {
		// 	log.Printf("error receiving next message: %v\n", er)
		// 	time.Sleep(time.Second)
		// 	continue
		// }
		if er != nil || len(evts.GetEvents()) == 0 {
			log.Printf("nothing to receive, pausing for %v (%v)\n", time.Duration(backoff), er)
			time.Sleep(time.Duration(backoff))
			if backoff < int64(maximumWait) {
				cycle++
				backoff += int64(waitStep) * cycle
				if backoff > int64(maximumWait) {
					backoff = int64(maximumWait)
				}
			}
			continue
		}
		return evts
	}
}

func (p *Pipe) LoadNext() error {
	defer telemetry.TimeAndCount(telemetry.TimerStart(), "pipe", "grpc", "loadnext", p.logstep)

	for len(p.bufferedmessages) == 0 {
		p.bufferedmessages = p.getWithBackoff().GetEvents()
	}
	// log.Println("loadnext after loop")
	p.currentmessage = p.bufferedmessages[0]
	p.bufferedmessages = p.bufferedmessages[1:]

	// evts := p.getWithBackoff()
	// p.currentmessage = evts.GetEvents()[0]
	return nil
	// evts, er := p.pipeservice.Recv(context.Background(), p.receiveoptions)
	// if er != nil {
	// 	log.Printf("error receiving next message: %v\n", er)
	// 	return er
	// }
	// if len(evts.GetEvents()) == 0 {
	// 	log.Println("no events fetched")
	// 	return errors.New("no event to fetch")
	// }
	// p.currentmessage = evts.GetEvents()[0]
	// return nil
}

func (p *Pipe) CurrentMessage() *protomessages.Event {
	return p.currentmessage
}

func (p *Pipe) Ack(id string) error {
	defer telemetry.TimeAndCount(telemetry.TimerStart(), "pipe", "grpc", "ack", p.logstep)

	_, er := p.pipeservice.Ack(context.Background(), &protopipes.CompleteRequest{
		Step: p.receiveoptions.GetPipe(),
		XId:  id})
	return er
}

func (p *Pipe) Complete(id string) error {
	defer telemetry.TimeAndCount(telemetry.TimerStart(), "pipe", "grpc", "complete", p.logstep)

	_, er := p.pipeservice.Complete(context.Background(), &protopipes.CompleteRequest{
		Step: p.receiveoptions.GetPipe(),
		XId:  id})
	p.currentmessage = nil
	if er != nil {
		return er
	}
	return nil
}

func (p *Pipe) Log(id string, code int32, text string) error {
	defer telemetry.TimeAndCount(telemetry.TimerStart(), "pipe", "grpc", "log", p.logstep)

	resp, er := p.pipeservice.AppendLog(context.Background(), &protopipes.RouteLogRequest{
		XId: id,
		Log: &protomessages.RouteLog{
			Step:    p.receiveoptions.GetPipe(),
			Code:    code,
			Message: text,
		}})
	if er != nil {
		return er
	}
	if !resp.GetOK() {
		return errors.New(resp.GetMessage())
	}
	return nil
}

func (p *Pipe) AddSteps(id string, steps []string) error {
	defer telemetry.TimeAndCount(telemetry.TimerStart(), "pipe", "grpc", "addsteps", p.logstep)

	resp, er := p.pipeservice.AddSteps(context.Background(), &protopipes.AddStepsRequest{
		XId:      id,
		After:    p.receiveoptions.GetPipe(),
		NewSteps: steps,
	})
	if er != nil {
		return er
	}
	if !resp.GetOK() {
		return errors.New(resp.GetMessage())
	}
	return nil
}

func (p *Pipe) Decorate(id string, decorations []*protopipes.Decoration) error {
	defer telemetry.TimeAndCount(telemetry.TimerStart(), "pipe", "grpc", "decorate", p.logstep)

	resp, er := p.pipeservice.Decorate(context.Background(), &protopipes.Decorations{
		XId:         id,
		Decorations: decorations,
	})
	if er != nil {
		return er
	}

	for _, res := range resp.GetGenericResponses() {
		if res.GetOK() {
			return errors.New(res.GetMessage())
		}
	}
	return nil
}
