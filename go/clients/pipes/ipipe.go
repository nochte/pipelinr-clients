package pipes

import (
	protomessages "github.com/nochte/pipelinr-protocol/protobuf/messages"
	protopipes "github.com/nochte/pipelinr-protocol/protobuf/pipes"
)

// clients.Pipe is the interface definition for a client
//  implementation of a pipe
// the key differentiator is that concrete implementations
//  of this interface must hold exactly one message, and
//  provide methods to interact with the message
type Pipe interface {
	// Configuration
	// SetReceiveOptions should set the global receive
	//  options for this pipe's receive operations
	SetReceiveOptions(*protopipes.ReceiveOptions)

	// Sending
	// Send will send a MessageEnvelop to this pipe
	Send(*protomessages.MessageEnvelop) (string, error)

	// Receiving
	// LoadNext will call recv on the underlying pipe,
	//  and load the message into this Pipe's memory
	LoadNext() error
	// Ack will acknowledge the current Pipe's message
	Ack(string) error
	// CurrentMessage will return the Pipe's current message
	CurrentMessage() *protomessages.Event
	// Complete will mark the message as complete and
	//  clear the Pipe's memory
	Complete(string) error
	// Log takes a code and text and log it to the Pipe's message
	Log(string, int32, string) error
	// AddSteps will add the list of steps after this Pipe's message's current step
	AddSteps(string, []string) error
	// Decorate will decorate the Pipe's message
	Decorate(string, []*protopipes.Decoration) error
}
