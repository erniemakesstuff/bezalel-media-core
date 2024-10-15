package publisherdrivers

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	dal "github.com/bezalel-media-core/v2/dal"
	tables "github.com/bezalel-media-core/v2/dal/tables/v1"
	manifest "github.com/bezalel-media-core/v2/manifest"
	"github.com/michimani/gotwi"
	"github.com/michimani/gotwi/tweet/managetweet"
	"github.com/michimani/gotwi/tweet/managetweet/types"
)

type TwitterDriver struct{}

type TwitterPostContents struct {
	TweetTextBody string
}

func (s TwitterDriver) Publish(pubCommand PublishCommand) error {
	acc, err := dal.GetPublisherAccount(pubCommand.RootPublishEvent.OwnerAccountID, pubCommand.RootPublishEvent.PublisherProfileID)
	if err != nil {
		log.Printf("correlationID: %s error loading publisher account for medium driver: %s", pubCommand.RootPublishEvent.LedgerID, err)
		return err
	}
	/*
		blogPayload, err := s.loadMediaContents(pubCommand.FinalRenderMediaRoot)
		if err != nil {
			log.Printf("correlationID: %s error downloading content for blog: %s", pubCommand.RootPublishEvent.LedgerID, err)
			return err
		}
	*/

	blogPayload := TwitterPostContents{
		TweetTextBody: "Hello World",
	}
	err = s.publishTwitterPost(pubCommand.RootPublishEvent.LedgerID, acc, blogPayload)
	if err != nil {
		log.Printf("correlationID: %s error uploading blog contents to Medium: %s", pubCommand.RootPublishEvent.LedgerID, err)
		return err
	}
	return err
}

func (s TwitterDriver) loadMediaContents(mediaEvent tables.MediaEvent) (TwitterPostContents, error) {
	// TODO: allow enrichment with images.
	result := TwitterPostContents{}
	var err error
	//scriptPayload, err := s.loadScriptPayload(mediaEvent)
	if err != nil {
		log.Printf("correlationID: %s error initializing medium blog contents: %s", mediaEvent.LedgerID, err)
		return result, err
	}

	//result.BlogTitle = scriptPayload.BlogTitle
	//result.HtmlBody = scriptPayload.BlogHtml
	return result, err
}

func (s TwitterDriver) loadScriptPayload(rootFinalRender tables.MediaEvent) (manifest.BlogJsonSchema, error) {
	payload, err := LoadAsString(rootFinalRender.ContentLookupKey)
	if err != nil {
		log.Printf("correlationID: %s error loading script content as string: %s", rootFinalRender.LedgerID, err)
		return manifest.BlogJsonSchema{}, err
	}
	return s.scriptPayloadToBlogJson(payload)
}

func (s TwitterDriver) scriptPayloadToBlogJson(payload string) (manifest.BlogJsonSchema, error) {
	result := manifest.BlogJsonSchema{}
	err := json.Unmarshal([]byte(payload), &result)
	if err != nil {
		log.Printf("error unmarshalling script text to blog schema object: %s", err)
		log.Printf("error payload: <%s>", payload)
		return result, err
	}

	if len(result.BlogHtml) == 0 {
		return manifest.BlogJsonSchema{}, fmt.Errorf("empty payload received: %s", payload)
	}

	return result, err
}

func (s TwitterDriver) publishTwitterPost(ledgerId string, account tables.AccountPublisher, tweetPayload TwitterPostContents) error {
	// TODO: Move PublisherAPISecretID to be a global-service config.
	// Retain the AccountPublisher fields; necessary.
	// https://trello.com/c/ol3Lvvop
	in := &gotwi.NewClientInput{
		AuthenticationMethod: gotwi.AuthenMethodOAuth1UserContext,
		OAuthToken:           account.UserAccessToken,
		OAuthTokenSecret:     account.UserAccessTokenSecret,
		APIKey:               account.PublisherAPISecretID,
		APIKeySecret:         account.PublisherAPISecretKey,
	}

	c, err := gotwi.NewClient(in)
	if err != nil {
		log.Printf("correlationID: %s error creating twitter client: %s", ledgerId, err)
	}
	p := &types.CreateInput{
		Text: gotwi.String(tweetPayload.TweetTextBody),
	}

	res, err := managetweet.Create(context.Background(), c, p)
	if err != nil {
		return err
	}

	log.Printf("correlationID: %s tweeted: %s", ledgerId, gotwi.StringValue(res.Data.ID))
	return err
}
