package main

import (
	"fmt"
	"log"
	"net/http"

	dynamo_configuration "github.com/bezalel-media-core/v2/configuration/dynamo"
	ingestion_service "github.com/bezalel-media-core/v2/service/ingestion"
)

const route_health = "/health"
const route_source_prompt = "/v1/source/prompt"

func main() {
	http.HandleFunc(route_health, handlerHealthCheck)
	http.HandleFunc(route_source_prompt, handlerCustomPrompt)
	dynamo_configuration.Init()
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
