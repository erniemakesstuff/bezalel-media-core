package drivers

import (
	"encoding/json"
	"io"
	"log"
	"strings"
	"sync"

	tables "github.com/bezalel-media-core/v2/dal/tables/v1"
	models_v1 "github.com/bezalel-media-core/v2/service/ingestion/models/v1"
)

type ReactDriver struct {
	Source    string
	List      ConcurrentList
	MaxBuffer int
	Mutex     sync.Mutex
}

var mutex sync.Mutex
var sourceToDriver = make(map[string]*ReactDriver)

// TODO: create multiple ReactDriver instances
// Maintain six ReactDriver instances: short videos, long videos, short images, long images, short audio, long audio
func NewReactDriver(source string) Driver {
	mutex.Lock()
	defer mutex.Unlock()
	driverInst, ok := sourceToDriver[source]
	if ok && driverInst != nil {
		return driverInst
	}
	maxBuffer := 2
	if strings.Contains(source, "long") {
		maxBuffer = 20
	}
	driverNew := &ReactDriver{Source: source,
		List: *NewConcurrentList(), MaxBuffer: maxBuffer, Mutex: sync.Mutex{}}
	sourceToDriver[source] = driverNew
	return driverNew
}

func (d ReactDriver) WithMedia(payloadIO io.ReadCloser) error {
	rawEvent, err := d.decode(payloadIO)
	if err != nil {
		log.Printf("error decoding raw event payload: %s", err)
	}
	d.List.Add(rawEvent)
	return nil
}

func (d ReactDriver) IsReady() bool {
	return d.List.Size() >= d.MaxBuffer
}

func (d ReactDriver) BuildEventPayload() (tables.Ledger, error) {
	d.Mutex.Lock()
	defer d.Mutex.Unlock()
	firstEntry, _ := d.List.Get(0)
	request := firstEntry.(models_v1.ReactionRequest)
	mediaUrls := []string{}
	for i := 0; i < d.List.Size(); i++ {
		entry, _ := d.List.Get(i)
		requestEntry := entry.(models_v1.ReactionRequest)
		mediaUrls = append(mediaUrls, requestEntry.ContentUrl)
	}
	d.List.Flush()
	return newLedgerFromUrls(request.TargetLanguage, mediaUrls, request.Source), nil
}

func (d ReactDriver) decode(payloadIO io.ReadCloser) (models_v1.ReactionRequest, error) {
	decoder := json.NewDecoder(payloadIO)
	var payload models_v1.ReactionRequest
	err := decoder.Decode(&payload)
	if err != nil {
		return payload, err
	}
	return payload, err
}
