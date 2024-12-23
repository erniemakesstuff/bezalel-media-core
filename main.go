package main

import (
	"log"
	"net/http"

	config "github.com/bezalel-media-core/v2/configuration"
	dynamo_configuration "github.com/bezalel-media-core/v2/configuration/dynamo"
	handlers "github.com/bezalel-media-core/v2/handlers"
	manifest "github.com/bezalel-media-core/v2/manifest"

	pubsub "github.com/bezalel-media-core/v2/service/orchestration"
	heartbeatDaemon "github.com/bezalel-media-core/v2/service/system/heartbeat"
)

const route_health = "/health"

// Oauth2 Flows
const route_youtube_oauth_start = "/v1/authcode/youtube" // start endpoint for enabling oauth code flow.
const route_youtube_oauth_callback = "/v1/authcode/youtube/callback"

// Custom ingestion sources
const route_source_prompt = "/v1/source/prompt"
const route_source_blog = "/v1/source/blog"
const route_source_forum = "/v1/source/forum"
const route_source_reactions_short_image = "/v1/source/reaction/short/image"
const route_source_reactions_short_video = "/v1/source/reaction/short/video"
const route_source_reactions_long_image = "/v1/source/reaction/long/image"
const route_source_reactions_long_video = "/v1/source/reaction/long/video"

func main() {
	// Register Oauth callbacks
	http.HandleFunc(route_youtube_oauth_start, handlers.HandlerOauthCodeFlowStart)
	http.HandleFunc(route_youtube_oauth_callback, handlers.HandlerOauthCodeCallback)
	// Register ingestion handlers
	http.HandleFunc(route_health, handlers.HandlerHealthCheck)
	http.HandleFunc(route_source_prompt, handlers.HandlerCustomPrompt)
	http.HandleFunc(route_source_blog, handlers.HandlerCustomPrompt)
	http.HandleFunc(route_source_forum, handlers.HandlerCustomPrompt)

	config.GetEnvConfigs()
	dynamo_configuration.Init()
	manifest.GetManifestLoader()
	go pubsub.PollForLedgerUpdates()
	go heartbeatDaemon.StartHeartbeatWatch()
	//go scaler.StartWatching() TODO Set this when ECS provisioned.
	log.Fatal(http.ListenAndServe(":8080", nil))
}
