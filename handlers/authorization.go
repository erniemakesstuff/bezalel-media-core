package handlers

import (
	"context"
	"log"

	"google.golang.org/api/youtube/v3"
)

func start() {
	GetClient(youtube.YoutubeReadonlyScope)
	/**
		New creates a new Service. It uses the provided http.Client for requests.

	Deprecated: please use NewService instead. To provide a custom HTTP client, use option.WithHTTPClient.
	If you are using google.golang.org/api/googleapis/transport.APIKey, use option.WithAPIKey with NewService instead.
	*/
	//youtube.New()
	_, err := youtube.NewService(context.Background())
	if err != nil {
		log.Fatalf("Error creating YouTube client: %v", err)
	}
}
