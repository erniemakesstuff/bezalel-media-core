package handlers

import (
	"fmt"
	"net/http"

	ingestion_service "github.com/bezalel-media-core/v2/service/ingestion"
)

func HandlerHealthCheck(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Ok")
}

func isAuthorized(r *http.Request) bool {
	return r.Header.Get("Authorization") == "password" // TODO, obviously replace this.
}

func HandlerCustomPrompt(w http.ResponseWriter, r *http.Request) {
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
