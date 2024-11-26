package main

import (
	"log"
	"net/http"

	config "github.com/bezalel-media-core/v2/configuration"
	dynamo_configuration "github.com/bezalel-media-core/v2/configuration/dynamo"
	handlers "github.com/bezalel-media-core/v2/handlers"
	manifest "github.com/bezalel-media-core/v2/manifest"
	pubsub "github.com/bezalel-media-core/v2/service/orchestration"
)

const route_health = "/health"
const route_source_prompt = "/v1/source/prompt"
const route_source_blog = "/v1/source/blog"
const route_source_forum = "/v1/source/forum"

func main() {
	http.HandleFunc(route_health, handlers.HandlerHealthCheck)
	http.HandleFunc(route_source_prompt, handlers.HandlerCustomPrompt)
	http.HandleFunc(route_source_blog, handlers.HandlerCustomPrompt)
	http.HandleFunc(route_source_forum, handlers.HandlerCustomPrompt)

	config.GetEnvConfigs()
	dynamo_configuration.Init()
	manifest.GetManifestLoader()
	go pubsub.PollForLedgerUpdates()
	//go scaler.StartWatching() TODO Set this when ECS provisioned.
	log.Fatal(http.ListenAndServe(":8080", nil))
}
