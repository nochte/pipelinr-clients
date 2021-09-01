package pipes

import (
	"context"
	"errors"
	"log"

	protomessages "github.com/nochte/pipelinr-protocol/protobuf/messages"
	protopipes "github.com/nochte/pipelinr-protocol/protobuf/pipes"
	"github.com/nochte/pipelinr/services"
	"github.com/nochte/pipelinr/telemetry"
)

type Pipe struct {
	receiveoptions *protopipes.ReceiveOptions
	logstep        string
	service        *services.PipeService

	currentmessage *protomessages.Event
}

func New(svc *services.PipeService, recvopts *protopipes.ReceiveOptions) *Pipe {
	logstep := "unknown"
	p := &Pipe{
		logstep: logstep,
		service: svc,
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
	defer telemetry.TimeAndCount(telemetry.TimerStart(), "pipe", "embedded", "send", p.logstep)
	out := protopipes.Xid{}
	er := p.service.Send(context.Background(), msg, &out)
	return out.GetXId(), er
}

func (p *Pipe) LoadNext() error {
	defer telemetry.TimeAndCount(telemetry.TimerStart(), "pipe", "embedded", "loadnext", p.logstep)
	evts := protomessages.Events{}
	er := p.service.Recv(context.Background(), p.receiveoptions, &evts)
	if er != nil {
		log.Printf("error receiving next message: %v\n", er)
		return er
	}
	if len(evts.GetEvents()) == 0 {
		log.Println("no events fetched")
		return errors.New("no event to fetch")
	}
	p.currentmessage = evts.GetEvents()[0]
	return nil
}

func (p *Pipe) CurrentMessage() *protomessages.Event {
	return p.currentmessage
}

func (p *Pipe) Ack(id string) error {
	defer telemetry.TimeAndCount(telemetry.TimerStart(), "pipe", "embedded", "ack", p.logstep)

	return p.service.Ack(context.Background(), &protopipes.CompleteRequest{
		Step: p.receiveoptions.GetPipe(),
		XId:  id}, &protopipes.GenericResponse{})
}

func (p *Pipe) Complete(id string) error {
	defer telemetry.TimeAndCount(telemetry.TimerStart(), "pipe", "embedded", "complete", p.logstep)

	er := p.service.Complete(context.Background(), &protopipes.CompleteRequest{
		Step: p.receiveoptions.GetPipe(),
		XId:  id}, &protopipes.GenericResponse{})
	p.currentmessage = nil
	if er != nil {
		return er
	}
	return nil
}

func (p *Pipe) Log(id string, code int32, text string) error {
	defer telemetry.TimeAndCount(telemetry.TimerStart(), "pipe", "embedded", "log", p.logstep)

	resp := protopipes.GenericResponse{}
	er := p.service.AppendLog(context.Background(), &protopipes.RouteLogRequest{
		XId: id,
		Log: &protomessages.RouteLog{
			Step:    p.receiveoptions.GetPipe(),
			Code:    code,
			Message: text,
		}}, &resp)
	if er != nil {
		return er
	}
	if !resp.GetOK() {
		return errors.New(resp.GetMessage())
	}
	return nil
}

func (p *Pipe) AddSteps(id string, steps []string) error {
	defer telemetry.TimeAndCount(telemetry.TimerStart(), "pipe", "embedded", "addsteps", p.logstep)

	resp := protopipes.GenericResponse{}
	er := p.service.AddSteps(context.Background(), &protopipes.AddStepsRequest{
		XId:      id,
		After:    p.receiveoptions.GetPipe(),
		NewSteps: steps,
	}, &resp)
	if er != nil {
		return er
	}
	if !resp.GetOK() {
		return errors.New(resp.GetMessage())
	}
	return nil
}

func (p *Pipe) Decorate(id string, decorations []*protopipes.Decoration) error {
	defer telemetry.TimeAndCount(telemetry.TimerStart(), "pipe", "embedded", "decorate", p.logstep)

	resp := protopipes.GenericResponses{}
	er := p.service.Decorate(context.Background(), &protopipes.Decorations{
		XId:         id,
		Decorations: decorations,
	}, &resp)
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
