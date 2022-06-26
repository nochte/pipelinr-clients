package drivers

import (
	"context"
	"errors"
	"log"
	"os"

	"github.com/nochte/pipelinr-protocol/protobuf/messages"
	"github.com/nochte/pipelinr-protocol/protobuf/pipes"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type GRPCDriver struct {
	client pipes.PipeClient
}

func NewGRPCDriver(url, apikey string) *GRPCDriver {
	if url == "" {
		url = os.Getenv("PIPELINR_GRPC_URL")
	}
	if url == "" {
		url = "grpc.pipelinr.dev:80"
	}
	if apikey == "" {
		apikey = os.Getenv("PIPELINR_API_KEY")
	}

	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())

	opts = append(opts, grpc.WithBlock())
	opts = append(opts, grpc.WithUnaryInterceptor(
		func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, handler grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			ctx = metadata.AppendToOutgoingContext(ctx, "authorization", apikey)
			handler(ctx, method, req, reply, cc, opts...)
			return nil
		}))

	conn, err := grpc.Dial(url, opts...)
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}

	pipesvc := pipes.NewPipeClient(conn)

	return &GRPCDriver{
		client: pipesvc,
	}
}

// Send takes at least a Payload and Route, returning the id of the message, error on fail
func (d GRPCDriver) Send(payload string, route []string) (string, error) {
	xid, er := d.client.Send(context.Background(), &messages.MessageEnvelop{
		Payload: payload,
		Route:   route,
	})
	if er != nil {
		return "", er
	}
	return xid.GetXId(), nil
}

// Recv takes a set of receive options, returning an array of events, error on fail
func (d GRPCDriver) Recv(receiveopts *pipes.ReceiveOptions) ([]*messages.Event, error) {
	evts, er := d.client.Recv(context.Background(), receiveopts)
	if er != nil {
		return nil, er
	}
	return evts.GetEvents(), nil
}

// Ack takes an id and a step, returning error on fail
func (d GRPCDriver) Ack(id, step string) error {
	_, er := d.client.Ack(context.Background(), &pipes.CompleteRequest{
		XId:  id,
		Step: step,
	})
	return er
}

// Complete takes an id and a step, return error on fail
func (d GRPCDriver) Complete(id, step string) error {
	res, er := d.client.Complete(context.Background(), &pipes.CompleteRequest{
		XId:  id,
		Step: step,
	})
	if er != nil {
		return er
	}
	if !res.GetOK() {
		return errors.New("step already completed")
	}
	return nil
}

// AppendLog takes an id, step, code, and message, returning error on fail
func (d GRPCDriver) AppendLog(id, step string, code int32, message string) error {
	_, er := d.client.AppendLog(context.Background(), &pipes.RouteLogRequest{
		XId: id,
		Log: &messages.RouteLog{
			Step:    step,
			Code:    code,
			Message: message,
		}})
	return er
}

// AddStepsAfter takes an id, step, and set of new steps, returning error on fail
func (d GRPCDriver) AddStepsAfter(id, after string, steps []string) error {
	_, er := d.client.AddSteps(context.Background(), &pipes.AddStepsRequest{
		XId:      id,
		After:    after,
		NewSteps: steps,
	})
	return er
}

// Decorate takes an id and set of set of decorations, returning error on fail
func (d GRPCDriver) Decorate(id string, decorations []*pipes.Decoration) []error {
	res, er := d.client.Decorate(context.Background(), &pipes.Decorations{
		XId:         id,
		Decorations: decorations,
	})
	if er != nil {
		return []error{er}
	}
	out := make([]error, len(decorations))
	for ndx, r := range res.GetGenericResponses() {
		if !r.GetOK() {
			out[ndx] = errors.New(r.GetMessage())
		}
	}
	return out
}

// GetDecorations takes an id and set of keys, returning the decorations for each key (null on no-key)
func (d GRPCDriver) GetDecorations(id string, keys []string) ([]*pipes.Decoration, error) {
	decs, er := d.client.GetDecorations(context.Background(), &pipes.GetDecorationRequest{
		XId:  id,
		Keys: keys,
	})
	if er != nil {
		return nil, er
	}
	out := make([]*pipes.Decoration, len(decs.GetDecorations()))
	for ndx := range decs.GetDecorations() {
		if decs.GetDecorations()[ndx].GetValue() != "" {
			out[ndx] = decs.GetDecorations()[ndx]
		} else {
			out[ndx] = nil
		}
	}
	return out, nil
}
