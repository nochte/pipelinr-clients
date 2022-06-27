package drivers

import (
	"github.com/nochte/pipelinr-protocol/protobuf/messages"
	"github.com/nochte/pipelinr-protocol/protobuf/pipes"
)

type Driver interface {
	// Send takes at least a Payload and Route, returning the id of the message, error on fail
	Send(payload string, route []string) (string, error)
	// Recv takes a set of receive options, returning an array of events, error on fail
	Recv(receiveopts *pipes.ReceiveOptions) ([]*messages.Event, error)
	// Ack takes an id and a step, returning error on fail
	Ack(id, step string) error
	// Complete takes an id and a step, return error on fail
	Complete(id, step string) error
	// AppendLog takes an id, step, code, and message, returning error on fail
	AppendLog(id, step string, code int32, message string) error
	// AddStepsAfter takes an id, step, and set of new steps, returning error on fail
	AddStepsAfter(id, after string, steps []string) error
	// Decorate takes an id and set of set of decorations, returning error on fail
	Decorate(id string, decorations []*pipes.Decoration) []error
	// GetDecorations takes an id and set of keys, returning the decorations for each key (null on no-key)
	GetDecorations(id string, keys []string) ([]*pipes.Decoration, error)
}
