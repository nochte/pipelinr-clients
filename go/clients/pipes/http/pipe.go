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

	// "net/url"
	"strings"
	"sync"
	"time"

	protomessages "github.com/nochte/pipelinr-protocol/protobuf/messages"
	protopipes "github.com/nochte/pipelinr-protocol/protobuf/pipes"
	"github.com/nochte/pipelinr/telemetry"
)

type Pipe struct {
	urlbase          string
	version          string
	step             string
	apikey           string
	currentmessage   *protomessages.Event
	receiveoptions   *protopipes.ReceiveOptions
	bufferedmessages []*protomessages.Event
	client           *http.Client
	clientmux        sync.Mutex
}

func New(urlbase, version, step, apikey string) *Pipe {
	return &Pipe{
		urlbase:        urlbase,
		version:        version,
		step:           step,
		apikey:         apikey,
		receiveoptions: &protopipes.ReceiveOptions{}}
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

func (p *Pipe) SetReceiveOptions(recvopts *protopipes.ReceiveOptions) {
	p.receiveoptions = recvopts
	if p.receiveoptions.GetPipe() != "" {
		p.step = p.receiveoptions.GetPipe()
	}
	if p.receiveoptions.Count < 1 {
		p.receiveoptions.Count = 1
	}
}

type HTTPResponse struct {
	Topic  string `json:"topic"`
	Text   string `json:"text"`
	Status int    `json:"status"`
}
type HTTPResponses []HTTPResponse

func (p *Pipe) Send(msg *protomessages.MessageEnvelop) (string, error) {
	defer telemetry.TimeAndCount(telemetry.TimerStart(), "pipe", "http", "send", p.step)
	bts, er := json.Marshal(msg)
	if er != nil {
		log.Printf("error marshaling to json %v\n", er)
		return "", er
	}
	response, er := p.getClient().Do(p.baseRequest("POST", fmt.Sprintf("%v/api/%v/pipes", p.urlbase, p.version), string(bts)))
	// response, er := p.getClient().Post(
	// 	fmt.Sprintf("%v/api/%v/pipes", p.urlbase, p.version),
	// 	"application/json",
	// 	bytes.NewBuffer(bts),
	// )
	if er != nil {
		log.Printf("error sending request %v\n", er)
		return "", er
	}

	body, err := ioutil.ReadAll(response.Body)
	defer response.Body.Close()
	if err != nil {
		log.Printf("error reading body %v\n", err)
		return "", nil
	}

	var outobj HTTPResponse
	er = json.Unmarshal(body, &outobj)
	if outobj.Status != 200 {
		log.Printf("error on ingest %v (%v)\n", outobj, er)
		return "", errors.New(outobj.Text)
	}
	return outobj.Text, nil
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

func (p *Pipe) fetch() (*protomessages.Events, error) {
	evts := protomessages.Events{}
	path := fmt.Sprintf("%v/api/%v/pipe/%v?%v", p.urlbase, p.version, p.step, p.urlParameters())
	response, er := p.getClient().Do(p.baseRequest("GET", path, ""))
	// response, er := p.getClient().Get(path)

	if er != nil {
		log.Printf("error receiving next message %v\n", er)
		return nil, er
	}

	body, er := ioutil.ReadAll(response.Body)
	defer response.Body.Close()
	if er != nil {
		log.Printf("error reading body %v\n", er)
		return nil, er
	}

	er = json.Unmarshal(body, &evts)
	if er != nil {
		log.Printf("error unmarshaling json %v\n", er)
		return nil, er
	}

	return &evts, nil
}

func (p *Pipe) getWithBackoff() *protomessages.Events {
	backoff := int64(initialWait)
	cycle := int64(0)
	for {
		evts, er := p.fetch()

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
	defer telemetry.TimeAndCount(telemetry.TimerStart(), "pipe", "http", "loadnext", p.step)

	// evts := p.getWithBackoff()
	// p.currentmessage = evts.GetEvents()[0]
	// log.Println("loadnext")
	for len(p.bufferedmessages) == 0 {
		p.bufferedmessages = p.getWithBackoff().GetEvents()
	}
	// log.Println("loadnext after loop")
	p.currentmessage = p.bufferedmessages[0]
	p.bufferedmessages = p.bufferedmessages[1:]
	return nil
}

func (p *Pipe) CurrentMessage() *protomessages.Event {
	return p.currentmessage
}

func (p *Pipe) Ack(id string) error {
	defer telemetry.TimeAndCount(telemetry.TimerStart(), "pipe", "http", "ack", p.step)

	// url, _ := url.Parse(fmt.Sprintf("%v/api/%v/message/%v/ack/%v", p.urlbase, p.version, id, p.step))
	u := fmt.Sprintf("%v/api/%v/message/%v/ack/%v", p.urlbase, p.version, id, p.step)
	response, er := p.getClient().Do(p.baseRequest("PUT", u, ""))
	// response, er := p.getClient().Do(&http.Request{
	// 	Method: "PUT",
	// 	URL:    url,
	// })
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

func (p *Pipe) Complete(id string) error {
	defer telemetry.TimeAndCount(telemetry.TimerStart(), "pipe", "http", "ack", p.step)

	// url, _ := url.Parse(fmt.Sprintf("%v/api/%v/message/%v/complete/%v", p.urlbase, p.version, id, p.step))
	// response, er := p.getClient().Do(&http.Request{
	// 	Method: "PUT",
	// 	URL:    url,
	// })
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
	p.currentmessage = nil
	defer response.Body.Close()

	var outobj HTTPResponse
	er = json.Unmarshal(body, &outobj)
	if outobj.Status != 200 {
		log.Printf("error on complete %v (%v)\n", outobj, er)
		return errors.New(outobj.Text)
	}
	return nil
}

func (p *Pipe) Log(id string, code int32, text string) error {
	defer telemetry.TimeAndCount(telemetry.TimerStart(), "pipe", "http", "log", p.step)

	bodybytes, _ := json.Marshal(map[string]interface{}{
		"Code":    code,
		"Message": text,
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

func (p *Pipe) AddSteps(id string, steps []string) error {
	defer telemetry.TimeAndCount(telemetry.TimerStart(), "pipe", "http", "addsteps", p.step)

	bodybytes, _ := json.Marshal(map[string]interface{}{
		"After":    p.step,
		"NewSteps": steps,
	})

	// url, _ := url.Parse(fmt.Sprintf("%v/api/%v/message/%v/route", p.urlbase, p.version, id))
	// response, er := p.getClient().Do(&http.Request{
	// 	Method: "PATCH",
	// 	URL:    url,
	// 	Body:   ioutil.NopCloser(bytes.NewReader(bodybytes)),
	// })
	u := fmt.Sprintf("%v/api/%v/message/%v/route", p.urlbase, p.version, id)
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

func (p *Pipe) Decorate(id string, decorations []*protopipes.Decoration) error {
	defer telemetry.TimeAndCount(telemetry.TimerStart(), "pipe", "http", "decorate", p.step)

	bodybytes, _ := json.Marshal(map[string]interface{}{"Decorations": decorations})

	// url, _ := url.Parse(fmt.Sprintf("%v/api/%v/message/%v/decorations", p.urlbase, p.version, id))
	// response, er := p.getClient().Do(&http.Request{
	// 	Method: "PATCH",
	// 	URL:    url,
	// 	Body:   ioutil.NopCloser(bytes.NewReader(bodybytes)),
	// })
	u := fmt.Sprintf("%v/api/%v/message/%v/decorations", p.urlbase, p.version, id)
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
