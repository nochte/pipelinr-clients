package pipes

import (
	protomessages "github.com/nochte/pipelinr-protocol/protobuf/messages"
	protopipes "github.com/nochte/pipelinr-protocol/protobuf/pipes"
)

// clients.Pipe is the interface definition for a client
//  implementation of a pipe
// Usage:
//  instantiate a pipe (pipe-specific)
//  get a handle on the Chan
//  run Start; handle an error (usually a fatal)
type Pipe interface {
	// SetReceiveOptions is the
	SetReceiveOptions(*protopipes.ReceiveOptions)
	// Start starts the pull goroutine - whenever the chan (returned by chan) is not full
	//  it will request more messages to process
	Start() error
	// Stop ... stops it. Dumbass.
	Stop()

	// Sending
	// Send will send a MessageEnvelop to this pipe, returning its ID
	Send(*protomessages.MessageEnvelop) (string, error)

	// Receiving
	// Chan returns a chan of Events
	Chan() chan *protomessages.Event

	// Modifying
	// Ack acknowledges the given message envelop by id
	Ack(id string) error

	// Complete marks it as complete
	Complete(id string) error

	// Log takes an id, code, and text and sends a log message
	Log(id string, code int32, message string) error

	// AddSteps will add the list of steps after the given id's current step
	AddSteps(id string, steps []string) error

	// Decorate will decorate a message by id
	Decorate(id string, decorations []*protopipes.Decoration) error

	// Idle returns true if there are no messages queued in the pipe
	Idle() bool
}
