package pipe

import (
	"errors"
	"log"
	"os"
	"time"

	"github.com/nochte/pipelinr-clients/go/lib/retry"
	"github.com/nochte/pipelinr-clients/go/pipe/drivers"
	"github.com/nochte/pipelinr-protocol/protobuf/messages"
	"github.com/nochte/pipelinr-protocol/protobuf/pipes"
)

type Pipe struct {
	driver         drivers.Driver
	step           string
	receiveOptions *pipes.ReceiveOptions
	attemptCount   int
	backoffMs      int
	running        bool
	stopped        bool
	messages       chan *messages.Event
}

func New(driver drivers.Driver, step string) *Pipe {
	return &Pipe{
		driver: driver,
		step:   step,
		receiveOptions: &pipes.ReceiveOptions{
			Pipe:                    step,
			AutoAck:                 false,
			Block:                   false,
			ExcludeRouting:          false,
			ExcludeRouteLog:         false,
			ExcludeDecoratedPayload: false,
			Count:                   1,
			Timeout:                 0,
			RedeliveryTimeout:       0,
		},
		attemptCount: 10,
		backoffMs:    250,
		running:      false,
		stopped:      false,
		// TODO: figure out the best way to transport this chan around
		// messages:     make(chan *messages.Event, 10),
	}
}

func NewHTTP(url, apikey, step string) *Pipe {
	return New(drivers.NewHTTPDriver(url, apikey), step)
}

func NewGRPC(url, apikey, step string) *Pipe {
	return New(drivers.NewGRPCDriver(url, apikey), step)
}

func (p Pipe) Name() string {
	return p.step
}

func (p Pipe) Step() string {
	return p.step
}

func (p *Pipe) Stop() {
	p.running = false
	p.stopped = true
	if p.messages != nil {
		close(p.messages)
	}
	p.messages = nil
}

func (p Pipe) ReceiveOptions() *pipes.ReceiveOptions {
	return p.receiveOptions
}

// SetReceiveOptions takes the receive options fields and overwrites what the pipe was using previously
//  use nil for a value to indicate that the pipe should continue to use what it has
func (p *Pipe) SetReceiveOptions(count *int32, timeout *int64, redeliveryTimeout *int64, autoAck, block, excludeRouting, excludeRouteLog, excludeDecoratedPayload *bool) {
	ro := p.receiveOptions
	if count != nil {
		ro.Count = *count
	}
	if timeout != nil {
		ro.Timeout = *timeout
	}
	if redeliveryTimeout != nil {
		ro.RedeliveryTimeout = *redeliveryTimeout
	}
	if autoAck != nil {
		ro.AutoAck = *autoAck
	}
	if block != nil {
		ro.Block = *block
	}
	if excludeRouting != nil {
		ro.ExcludeRouting = *excludeRouting
	}
	if excludeRouteLog != nil {
		ro.ExcludeRouteLog = *excludeRouteLog
	}
	if excludeDecoratedPayload != nil {
		ro.ExcludeDecoratedPayload = *excludeDecoratedPayload
	}

	p.receiveOptions = ro
}

func (p *Pipe) SetRetryPolicy(attemptcount, backoffMs int) {
	p.attemptCount = attemptcount
	p.backoffMs = backoffMs
}

// Send takes a payload and route, and submits it to pipelinr, returning the id of the event or error on failure
func (p Pipe) Send(payload string, route []string) (string, error) {
	if payload == "" {
		return "", errors.New("payload required")
	}
	if len(route) == 0 {
		return "", errors.New("route must have at least 1 element")
	}

	var out string
	er := retry.Do(func() error {
		res, er := p.driver.Send(payload, route)
		out = res
		return er
	}, p.attemptCount, time.Duration(p.backoffMs))

	return out, er
}

// Ack acknowledges a message on this pipe, returning error on failure
func (p Pipe) Ack(id string) error {
	return p.driver.Ack(id, p.step)
}

// Complete complets a message on this pipe, returning error on failure
func (p Pipe) Complete(id string) error {
	return p.driver.Complete(id, p.step)
}

// Log logs to a message with this pipe's step
func (p Pipe) Log(id string, code int32, message string) error {
	if message == "" {
		return errors.New("message required")
	}

	return p.driver.AppendLog(id, p.step, code, message)
}

// AddSteps adds steps to this message after the pipe's step
func (p Pipe) AddSteps(id string, steps []string) error {
	if len(steps) == 0 {
		return errors.New("steps must be a slice with length > 0")
	}

	return p.driver.AddStepsAfter(id, p.step, steps)
}

// Decorate appends some decorations to a message
//  Note that overwriting keys is allowed and encouraged, where the last
//  Value written to the key will be what is presented later in the pipe
func (p Pipe) Decorate(id string, decorations []*pipes.Decoration) []error {
	if len(decorations) == 0 {
		return []error{errors.New("decorations must be a slice with length > 0")}
	}

	return p.driver.Decorate(id, decorations)
}

// GetDecorations returns a set of decorations, where the values will be json-encoded values
//  - ex: string type: `"some-string"`
//  - ex: int type: `1`
//  - ex: json type: `{"some":{"things":{"go":"here"}}}`
func (p Pipe) GetDecorations(id string, keys []string) ([]*pipes.Decoration, error) {
	if len(keys) == 0 {
		return nil, errors.New("keys must be a slice with length > 0")
	}

	return p.driver.GetDecorations(id, keys)
}

// Fetch will retrieve messages from this pipe with the pipes receive options
func (p Pipe) Fetch() ([]*messages.Event, error) {
	return p.driver.Recv(p.receiveOptions)
}

func (p Pipe) fetchWithBackoff() ([]*messages.Event, error) {
	startTime := time.Now()
	iterations := 0

	var out []*messages.Event
	er := retry.Do(func() error {
		if p.stopped {
			out = nil
			return nil
		}
		if iterations > 0 && os.Getenv("PIPELINR_DEBUG") != "" {
			log.Printf("trying %v, elapsed %v, iteration %v\n", p.step, time.Since(startTime), iterations)
		}
		iterations++
		o, er := p.driver.Recv(p.receiveOptions)
		if er != nil {
			return er
		}
		if len(o) == 0 {
			return errors.New("not found")
		}
		out = o
		return nil
	}, p.attemptCount, time.Duration(p.backoffMs))

	return out, er
}

// Start starts a goroutine that fetches messages from pipelinr to be processed
//  It will retrieve up to receiveOptions.Count number of messages and make them available
//  in this pipe's Chan
// If maxmessages is > 0, then this pipe will auto-stop when maxmessages has been reached
//  which has no practical purpose beyond testing scenarios. Best not to use it in production
// Start returns an error if the pipe is already running
func (p *Pipe) Start(maxmessages int) error {
	if p.running {
		return errors.New("already running")
	}

	p.running = true
	processedMessages := 0
	startTime := time.Now()

	p.messages = make(chan *messages.Event, p.receiveOptions.GetCount())

	for p.running {
		if os.Getenv("PIPELINR_DEBUG") != "" {
			log.Printf("%v pipe is not full, fetching some - enqueued %v - total processed %v\n", p.step, len(p.messages), processedMessages)
		}

		evts, er := p.fetchWithBackoff()
		if er != nil {
			if os.Getenv("PIPELINR_DEBUG") != "" {
				log.Printf("%v error on fetch wtih backoff: %v\n", p.step, er)
			}
			time.Sleep(time.Second)

			continue
		}

		for _, evt := range evts {
			processedMessages++
			p.messages <- evt
		}

		if maxmessages > 0 && processedMessages >= maxmessages {
			log.Printf("%v processed through all messages in %v\n", p.step, time.Since(startTime))
			p.Stop()
		}
	}

	return nil
}

// Chan returns the internal p.messages chan, such that the caller is able to process messages in sequence
func (p *Pipe) Chan() chan *messages.Event {
	for p.messages == nil {
		time.Sleep(time.Millisecond * 10)
	}
	return p.messages
}
