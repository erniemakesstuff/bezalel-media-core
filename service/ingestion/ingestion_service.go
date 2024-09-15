package ingestion

import (
	"encoding/json"
	"log"
	"net/http"

	source_events "github.com/bezalel-media-core/v2/service/ingestion/models/v1"
)

func HandleSourceEvent(source string, r *http.Request) error {
	decoder := json.NewDecoder(r.Body)
	var payload source_events.Raw_Event
	err := decoder.Decode(&payload)
	if err != nil {
		return err
	}
	payload.Source = source
	log.Println(payload)
	return err
}
