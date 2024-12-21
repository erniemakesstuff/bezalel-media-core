package handlers

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"

	authorization "github.com/bezalel-media-core/v2/service/authorization"
	requestModels "github.com/bezalel-media-core/v2/service/models"
)

func HandlerHealthCheck(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Ok")
}

func isAuthorized(r *http.Request) bool {
	return r.Header.Get("Authorization") == "password" // TODO, obviously replace this.
}

func HandlerOauthCodeFlowStart(w http.ResponseWriter, r *http.Request) {
	// TODO: Invoke via post from SPA
	// Handle redirect from SPA from the returned authUrl
	// Ensure during setup-wizard to incrementally save publisherProfile details; so state isn't lost on callback.
	decoder := json.NewDecoder(r.Body)
	var payload requestModels.AuthorizationCodeState
	err := decoder.Decode(&payload)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, err.Error())
		return
	}
	googleAuthClient := authorization.GoogleAuth{}
	authUrl, err := googleAuthClient.StartOauthCodeFlow(payload.AccountId, payload.PublisherProfileId)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, err.Error())
		return
	}
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "{\"authUrl\":\"%s\"", authUrl)
}

func HandlerOauthCodeCallback(w http.ResponseWriter, r *http.Request) {
	// TODO: Change this to a post endpoint
	// OauthCallback
	code := r.FormValue("code")
	state := r.FormValue("state")
	data, err := base64.StdEncoding.DecodeString(state)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, err.Error())
		return
	}
	var payload requestModels.AuthorizationCodeState
	json.Unmarshal(data, &payload)
	googleAuthClient := authorization.GoogleAuth{}
	_, err = googleAuthClient.StoreAuthorizationCode(code, payload.AccountId, payload.PublisherProfileId)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, err.Error())
		return
	}

	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintf(w, "Received code: %v\r\nYou can now safely close this browser window. Other payload: %s", code, payload.AccountId+":"+payload.PublisherProfileId)
}
