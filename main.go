package main

import (
	"fmt"
	"log"
	"net/http"

	config "github.com/bezalel-media-core/v2/configuration"
	dynamo_configuration "github.com/bezalel-media-core/v2/configuration/dynamo"
	manifest "github.com/bezalel-media-core/v2/manifest"
	ingestion_service "github.com/bezalel-media-core/v2/service/ingestion"
	pubsub "github.com/bezalel-media-core/v2/service/orchestration"
)

const route_health = "/health"
const route_source_prompt = "/v1/source/prompt"
const route_source_blog = "/v1/source/blog"

func main() {
	http.HandleFunc(route_health, handlerHealthCheck)
	http.HandleFunc(route_source_prompt, handlerCustomPrompt)
	http.HandleFunc(route_source_blog, handlerCustomPrompt)
	dynamo_configuration.Init()
	manifest.GetManifestLoader()
	config.GetEnvConfigs()
	go pubsub.PollForLedgerUpdates()
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func handlerHealthCheck(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Ok")
}

func isAuthorized(r *http.Request) bool {
	return r.Header.Get("Authorization") == "password" // TODO, obviously replace this.
}

func handlerCustomPrompt(w http.ResponseWriter, r *http.Request) {
	if !isAuthorized(r) {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w, "Unauthorized.")
		return
	}

	if r.Method != "POST" {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "Route must be called with POST, given %s", r.Method)
		return
	}
	source := r.URL.Path[1:]
	err := ingestion_service.SaveSourceEventToLedger(source, r)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, err.Error())
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Ok")
}
