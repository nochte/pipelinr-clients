package pipes

import (
	"context"
	"errors"
	"io"
	"log"
	"time"

	"github.com/nochte/pipelinr-lib/retry"
	protomessages "github.com/nochte/pipelinr-protocol/protobuf/messages"
	protopipes "github.com/nochte/pipelinr-protocol/protobuf/pipes"
	"google.golang.org/grpc"
)

type Pipe struct {
	url            string
	step           string
	apikey         string
	messages       chan *protomessages.Event
	receiveoptions *protopipes.ReceiveOptions
	running        bool
	client         protopipes.PipeClient
}

func New(url, step, apikey string) *Pipe {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())

	opts = append(opts, grpc.WithBlock())
	conn, err := grpc.Dial(url, opts...)
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}

	pipesvc := protopipes.NewPipeClient(conn)

	p := &Pipe{
		running: false,
		url:     url,
		step:    step,
		apikey:  apikey,
		client:  pipesvc,
		receiveoptions: &protopipes.ReceiveOptions{
			AutoAck:           false,
			Block:             true,
			Count:             10,
			Timeout:           5,
			Pipe:              step,
			RedeliveryTimeout: 60,
		}}

	go func() {
		for {
			log.Printf("DEBUG Backlog %v\n", p.InQueue())
			time.Sleep(time.Second)
		}
	}()

	return p
}

func (p *Pipe) SetReceiveOptions(opts *protopipes.ReceiveOptions) {
	p.receiveoptions = opts
	p.receiveoptions.Pipe = p.step
}

func (p *Pipe) Start() error {
	if p.running {
		return errors.New("already running")
	}
	p.running = true
	p.messages = make(chan *protomessages.Event, p.receiveoptions.Count)
	// for loop
	for p.running {
		stream, er := p.client.StreamRecv(context.Background(), p.receiveoptions)
		if er != nil {
			log.Printf("error opening stream %v\n", er)
			time.Sleep(time.Second)
			continue
		}
		for {
			elm, er := stream.Recv()
			if er == io.EOF {
				log.Printf("stream closed, reconnecting %v\n", er)
				time.Sleep(time.Second)
				break
			}
			if er != nil {
				log.Printf("error receiving message %v\n", er)
				time.Sleep(time.Second)
				break
			}
			if elm == nil {
				log.Println("nil element received")
				time.Sleep(time.Second)
				continue
			}
			p.messages <- elm
		}
	}
	//  send request for receiveoptions.Count, then load them into <- messages
	return nil
}

func (p *Pipe) Stop() {
	p.running = false
}

// Sending
// Send will send a MessageEnvelop to this pipe, returning its ID
func (p *Pipe) Send(msg *protomessages.MessageEnvelop) (string, error) {
	var id string

	er := retry.Do(func() error {
		xid, er := p.client.Send(context.Background(), msg)
		if er != nil {
			return er
		}
		id = xid.GetXId()
		return nil
	}, 10, time.Millisecond*100)
	return id, er
}

// Receiving
// Chan returns a chan of MessageEnvelops
func (p *Pipe) Chan() chan *protomessages.Event {
	return p.messages
}

// Modifying
// Ack acknowledges the given message envelop by id
func (p *Pipe) Ack(id string) error {
	_, er := p.client.Ack(context.Background(), &protopipes.CompleteRequest{
		XId:  id,
		Step: p.step,
	})
	return er
}

// Complete marks it as complete
func (p *Pipe) Complete(id string) error {
	_, er := p.client.Complete(context.Background(), &protopipes.CompleteRequest{
		XId:  id,
		Step: p.step,
	})
	return er
}

// Log takes an id, code, and text and sends a log message
func (p *Pipe) Log(id string, code int32, message string) error {
	_, er := p.client.AppendLog(context.Background(), &protopipes.RouteLogRequest{
		XId: id,
		Log: &protomessages.RouteLog{
			Step:    p.step,
			Code:    code,
			Message: message,
		}})
	return er
}

// AddSteps will add the list of steps after the given id's current step
func (p *Pipe) AddSteps(id string, steps []string) error {
	_, er := p.client.AddSteps(context.Background(), &protopipes.AddStepsRequest{
		After:    p.step,
		XId:      id,
		NewSteps: steps,
	})
	return er
}

// Decorate will decorate a message by id
func (p *Pipe) Decorate(id string, decorations []*protopipes.Decoration) error {
	_, er := p.client.Decorate(context.Background(), &protopipes.Decorations{
		XId:         id,
		Decorations: decorations,
	})
	return er
}

func (p *Pipe) Idle() bool {
	return len(p.messages) == 0
}

func (p *Pipe) InQueue() int {
	return len(p.messages)
}
