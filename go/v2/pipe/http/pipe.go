package pipes

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/nochte/pipelinr-lib/retry"
	protomessages "github.com/nochte/pipelinr-protocol/protobuf/messages"
	protopipes "github.com/nochte/pipelinr-protocol/protobuf/pipes"
)

type Pipe struct {
	urlbase        string
	version        string
	step           string
	apikey         string
	messages       chan *protomessages.Event
	receiveoptions *protopipes.ReceiveOptions
	client         *http.Client
	clientmux      sync.Mutex
	running        bool
}

// New defaults to:
// AutoAck = true;
// Block = true
// Count = 10;
// Timeout = 5;
// RedeliveryTimeout = 60;
func New(urlbase, version, step, apikey string) *Pipe {
	return &Pipe{
		running: false,
		urlbase: urlbase,
		version: version,
		step:    step,
		apikey:  apikey,
		receiveoptions: &protopipes.ReceiveOptions{
			AutoAck:           false,
			Block:             true,
			Count:             10,
			Timeout:           5,
			Pipe:              step,
			RedeliveryTimeout: 60,
		}}
}

func (p *Pipe) getClient() *http.Client {
	if p.client != nil {
		return p.client
	}
	p.clientmux.Lock()
	defer p.clientmux.Unlock()
	if p.client != nil {
		return p.client
	}
	var netTransport = &http.Transport{
		Dial: (&net.Dialer{
			Timeout: 5 * time.Second,
		}).Dial,
		TLSHandshakeTimeout: 5 * time.Second,
	}
	p.client = &http.Client{
		Timeout:   time.Second * 10,
		Transport: netTransport,
	}
	return p.client
}

func (p *Pipe) baseRequest(method, url, body string) *http.Request {
	var buffer io.Reader
	if body != "" {
		buffer = ioutil.NopCloser(bytes.NewReader([]byte(body)))
	}
	req, _ := http.NewRequest(method, url, buffer)
	req.Header.Set("authorization", fmt.Sprintf("api %v", p.apikey))

	return req
}

const initialWait = time.Millisecond
const waitStep = time.Millisecond * 100
const maximumWait = time.Second * 10

func (p *Pipe) urlParameters() string {
	parms := make([]string, 0, 5)
	if p.receiveoptions.AutoAck {
		parms = append(parms, "autoAck=yes")
	}
	if p.receiveoptions.Block {
		parms = append(parms, "block=yes")
	}
	if p.receiveoptions.Count > 0 {
		parms = append(parms, fmt.Sprintf("count=%v", p.receiveoptions.Count))
	}
	if p.receiveoptions.Timeout > 0 {
		parms = append(parms, fmt.Sprintf("timeout=%v", p.receiveoptions.Timeout))
	} else {
		parms = append(parms, "timeout=60")
	}
	if p.receiveoptions.RedeliveryTimeout > 0 {
		parms = append(parms, fmt.Sprintf("redeliveryTimeout=%v", p.receiveoptions.RedeliveryTimeout))
	}
	return strings.Join(parms, "&")
}

func (p *Pipe) fetchWithBackoff() (*protomessages.Events, error) {
	evts := protomessages.Events{}
	path := fmt.Sprintf("%v/api/%v/pipe/%v?%v", p.urlbase, p.version, p.step, p.urlParameters())

	er := retry.Do(func() error {
		response, er := p.getClient().Do(p.baseRequest("GET", path, ""))
		// response, er := p.getClient().Get(path)

		if er != nil {
			// log.Printf("error receiving next message %v\n", er)
			return er
		}

		body, er := ioutil.ReadAll(response.Body)
		defer response.Body.Close()
		if er != nil {
			log.Printf("error reading body %v\n", er)
			return er
		}

		er = json.Unmarshal(body, &evts)
		if er != nil {
			log.Printf("error unmarshaling json %v\n", er)
			return er
		}
		if len(evts.GetEvents()) == 0 {
			return errors.New("nothing to receive")
		}
		return nil
	}, 10, time.Millisecond*1000)

	return &evts, er
}

func (p *Pipe) get() *protomessages.Events {
	for {
		evts, er := p.fetchWithBackoff()
		if er == nil {
			return evts
		}
	}
}

type HTTPResponse struct {
	Topic  string `json:"topic"`
	Text   string `json:"text"`
	Status int    `json:"status"`
}
type HTTPResponses []HTTPResponse

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
		evts := p.get()
		for _, evt := range evts.GetEvents() {
			p.messages <- evt
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
	bts, er := json.Marshal(msg)
	if er != nil {
		log.Printf("error marshaling to json %v\n", er)
		return "", er
	}

	var id string
	er = retry.Do(func() error {
		response, er := p.getClient().Do(p.baseRequest("POST", fmt.Sprintf("%v/api/%v/pipes", p.urlbase, p.version), string(bts)))
		if er != nil {
			log.Printf("error sending request %v\n", er)
			return er
		}

		body, err := ioutil.ReadAll(response.Body)
		defer response.Body.Close()
		if err != nil {
			log.Printf("error reading body %v\n", err)
			return nil
		}

		var outobj HTTPResponse
		er = json.Unmarshal(body, &outobj)
		if outobj.Status != 200 {
			log.Printf("error on ingest %v (%v)\n", outobj, er)
			return errors.New(outobj.Text)
		}
		id = outobj.Text
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
	u := fmt.Sprintf("%v/api/%v/message/%v/ack/%v", p.urlbase, p.version, id, p.step)
	response, er := p.getClient().Do(p.baseRequest("PUT", u, ""))
	if er != nil {
		return er
	}

	body, er := ioutil.ReadAll(response.Body)
	if er != nil {
		log.Printf("error reading body %v\n", er)
		return er
	}
	defer response.Body.Close()

	var outobj HTTPResponse
	er = json.Unmarshal(body, &outobj)
	if outobj.Status != 200 {
		log.Printf("error on ack %v (%v)\n", outobj, er)
		return errors.New(outobj.Text)
	}
	return nil
}

// Complete marks it as complete
func (p *Pipe) Complete(id string) error {
	u := fmt.Sprintf("%v/api/%v/message/%v/complete/%v", p.urlbase, p.version, id, p.step)
	response, er := p.getClient().Do(p.baseRequest("PUT", u, ""))
	if er != nil {
		return er
	}

	body, er := ioutil.ReadAll(response.Body)
	if er != nil {
		log.Printf("error reading body %v\n", er)
		return er
	}
	defer response.Body.Close()

	var outobj HTTPResponse
	er = json.Unmarshal(body, &outobj)
	if outobj.Status != 200 {
		log.Printf("error on complete (%v) %v (%v)\n", id, outobj, er)
		return errors.New(outobj.Text)
	}
	return nil
}

// Log takes an id, code, and text and sends a log message
func (p *Pipe) Log(id string, code int32, message string) error {
	bodybytes, _ := json.Marshal(map[string]interface{}{
		"Code":    code,
		"Message": message,
	})

	// url, _ := url.Parse(fmt.Sprintf("%v/api/%v/message/%v/log/%v", p.urlbase, p.version, id, p.step))
	// response, er := p.getClient().Do(&http.Request{
	// 	Method: "PATCH",
	// 	URL:    url,
	// 	Body:   ioutil.NopCloser(bytes.NewReader(bodybytes)),
	// })
	u := fmt.Sprintf("%v/api/%v/message/%v/log/%v", p.urlbase, p.version, id, p.step)
	response, er := p.getClient().Do(p.baseRequest("PATCH", u, string(bodybytes)))

	if er != nil {
		return er
	}

	body, er := ioutil.ReadAll(response.Body)
	if er != nil {
		log.Printf("error reading body %v\n", er)
		return er
	}
	defer response.Body.Close()

	var outobj HTTPResponse
	er = json.Unmarshal(body, &outobj)
	if outobj.Status != 200 {
		log.Printf("error on log %v (%v)\n", outobj, er)
		return errors.New(outobj.Text)
	}

	return nil
}

// AddSteps will add the list of steps after the given id's current step
func (p *Pipe) AddSteps(id string, steps []string) error {
	u := fmt.Sprintf("%v/api/%v/message/%v/route", p.urlbase, p.version, id)
	bodybytes, _ := json.Marshal(map[string]interface{}{
		"After":    p.step,
		"NewSteps": steps,
	})
	response, er := p.getClient().Do(p.baseRequest("PATCH", u, string(bodybytes)))
	if er != nil {
		return er
	}

	body, er := ioutil.ReadAll(response.Body)
	if er != nil {
		log.Printf("error reading body %v\n", er)
		return er
	}
	defer response.Body.Close()

	var outobj HTTPResponse
	er = json.Unmarshal(body, &outobj)
	if outobj.Status != 200 {
		log.Printf("error on log %v (%v)\n", outobj, er)
		return errors.New(outobj.Text)
	}
	return nil
}

// Decorate will decorate a message by id
func (p *Pipe) Decorate(id string, decorations []*protopipes.Decoration) error {
	u := fmt.Sprintf("%v/api/%v/message/%v/decorations", p.urlbase, p.version, id)
	bodybytes, _ := json.Marshal(map[string]interface{}{"Decorations": decorations})
	response, er := p.getClient().Do(p.baseRequest("PATCH", u, string(bodybytes)))
	if er != nil {
		return er
	}

	body, er := ioutil.ReadAll(response.Body)
	if er != nil {
		log.Printf("error reading body %v\n", er)
		return er
	}
	defer response.Body.Close()

	var outobj HTTPResponses
	er = json.Unmarshal(body, &outobj)
	if er != nil {
		log.Printf("error unmarshaling decorate %v\n", er)
		return er
	}
	for _, oob := range outobj {
		if oob.Status != 200 {
			log.Printf("error on decorate %v\n", oob)
			return errors.New(oob.Text)
		}
	}

	return nil
}

func (p *Pipe) Idle() bool {
	return len(p.messages) == 0
}
