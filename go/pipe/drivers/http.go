package drivers

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/imroc/req/v3"
	"github.com/nochte/pipelinr-protocol/protobuf/messages"
	"github.com/nochte/pipelinr-protocol/protobuf/pipes"
)

type HTTPDriver struct {
	urlbase string
	apikey  string
	client  *req.Client
}

type HTTPResponse struct {
	Topic  string `json:"topic"`
	Text   string `json:"text"`
	Status int    `json:"status"`
}
type HTTPResponses []HTTPResponse

func NewHTTPDriver(url, apikey string) *HTTPDriver {
	if url == "" {
		url = os.Getenv("PIPELINR_URL")
	}
	if url == "" {
		url = "https://pipelinr.dev"
	}
	if apikey == "" {
		apikey = os.Getenv("PIPELINR_API_KEY")
	}

	re := req.C().
		SetUserAgent("cloc-test-suite-v2").
		SetTimeout(time.Second * 20)

	if os.Getenv("PIPELINR_DEBUG") != "" {
		re = re.DevMode()
	}

	return &HTTPDriver{
		urlbase: url,
		apikey:  apikey,
		client:  re,
	}
}

func (d HTTPDriver) baseRequest() *req.Request {
	return d.client.R().
		SetHeader("accept", "application/json").
		SetHeader("content-type", "application/json").
		SetHeader("authorization", fmt.Sprintf("api %v", d.apikey))
}

func (d HTTPDriver) Send(payload string, route []string) (string, error) {
	var result HTTPResponse
	_, er := d.baseRequest().
		SetResult(&result).
		SetBody(map[string]interface{}{
			"Payload": payload,
			"Route":   route}).
		Post(fmt.Sprintf("%v/api/2/pipes", d.urlbase))

	if er != nil {
		return "", er
	}

	return result.Text, nil
}

func (d HTTPDriver) Recv(receiveopts *pipes.ReceiveOptions) ([]*messages.Event, error) {
	queryparams := make(map[string]string)
	queryparams["pipe"] = receiveopts.GetPipe()
	queryparams["count"] = fmt.Sprintf("%v", receiveopts.GetCount())
	if receiveopts.GetTimeout() > 0 {
		queryparams["timeout"] = fmt.Sprintf("%v", receiveopts.GetTimeout())
	} else {
		queryparams["timeout"] = fmt.Sprintf("%v", 60)
	}
	if receiveopts.GetRedeliveryTimeout() > 0 {
		queryparams["redeliveryTimeout"] = fmt.Sprintf("%v", receiveopts.GetRedeliveryTimeout())
	}
	if receiveopts.GetAutoAck() {
		queryparams["autoAck"] = "yes"
	}
	if receiveopts.GetExcludeRouting() {
		queryparams["excludeRouting"] = "yes"
	}
	if receiveopts.GetExcludeRouteLog() {
		queryparams["excludeRouteLog"] = "yes"
	}
	if receiveopts.GetExcludeDecoratedPayload() {
		queryparams["excludeDecoratedPayload"] = "yes"
	}
	if receiveopts.GetBlock() {
		queryparams["block"] = "yes"
	}

	var result messages.Events
	_, er := d.baseRequest().
		SetResult(&result).
		SetQueryParams(queryparams).
		Get(fmt.Sprintf("%v/api/2/pipe/%v", d.urlbase, receiveopts.GetPipe()))

	if er != nil {
		return nil, er
	}
	return result.GetEvents(), nil
}

// Ack takes an id and a step, returning error on fail
func (d HTTPDriver) Ack(id, step string) error {
	_, er := d.baseRequest().
		Put(fmt.Sprintf("%v/api/2/message/%v/ack/%v", d.urlbase, id, step))
	return er
}

// Complete takes an id and a step, return error on fail
func (d HTTPDriver) Complete(id, step string) error {
	var result HTTPResponse
	_, er := d.baseRequest().
		SetResult(&result).
		Put(fmt.Sprintf("%v/api/2/message/%v/complete/%v", d.urlbase, id, step))

	if er != nil {
		return er
	}
	if result.Status != 200 {
		return errors.New("step already completed")
	}
	return nil
}

// AppendLog takes an id, step, code, and message, returning error on fail
func (d HTTPDriver) AppendLog(id, step string, code int32, message string) error {
	var result HTTPResponse
	_, er := d.baseRequest().
		SetResult(&result).
		SetBody(map[string]interface{}{
			"Code":    code,
			"Message": message}).
		Patch(fmt.Sprintf("%v/api/2/message/%v/log/%v", d.urlbase, id, step))

	if er != nil {
		return er
	}
	if result.Status != 200 {
		return errors.New(result.Text)
	}
	return nil
}

// AddStepsAfter takes an id, step, and set of new steps, returning error on fail
func (d HTTPDriver) AddStepsAfter(id, after string, steps []string) error {
	var result HTTPResponse
	_, er := d.baseRequest().
		SetResult(&result).
		SetBody(map[string]interface{}{
			"After":    after,
			"NewSteps": steps}).
		Patch(fmt.Sprintf("%v/api/2/message/%v/route", d.urlbase, id))

	if er != nil {
		return er
	}
	if result.Status != 200 {
		return errors.New(result.Text)
	}
	return nil
}

// Decorate takes an id and set of set of decorations, returning error on fail
func (d HTTPDriver) Decorate(id string, decorations []*pipes.Decoration) []error {
	var results HTTPResponses
	_, er := d.baseRequest().
		SetResult(&results).
		SetBody(map[string]interface{}{
			"Decorations": decorations}).
		Patch(fmt.Sprintf("%v/api/2/message/%v/decorations", d.urlbase, id))

	if er != nil {
		return []error{er}
	}
	out := make([]error, len(decorations))
	for ndx, r := range results {
		if r.Status != 200 {
			out[ndx] = errors.New(r.Text)
		}
	}
	return out
}

// GetDecorations takes an id and set of keys, returning the decorations for each key (null on no-key)
func (d HTTPDriver) GetDecorations(id string, keys []string) ([]*pipes.Decoration, error) {
	var result pipes.Decorations
	_, er := d.baseRequest().
		SetResult(&result).
		SetQueryParams(map[string]string{
			"keys": strings.Join(keys, ",")}).
		Get(fmt.Sprintf("%v/api/2/message/%v/decorations", d.urlbase, id))

	if er != nil {
		return nil, er
	}
	out := make([]*pipes.Decoration, len(result.GetDecorations()))
	for ndx := range result.GetDecorations() {
		if result.GetDecorations()[ndx].GetValue() != "" {
			out[ndx] = result.GetDecorations()[ndx]
		} else {
			out[ndx] = nil
		}
	}
	return out, nil
}
