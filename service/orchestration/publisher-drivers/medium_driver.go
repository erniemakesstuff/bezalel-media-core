package publisherdrivers

import (
	"encoding/json"
	"fmt"
	"log"

	dal "github.com/bezalel-media-core/v2/dal"
	tables "github.com/bezalel-media-core/v2/dal/tables/v1"
	manifest "github.com/bezalel-media-core/v2/manifest"
	medium "github.com/medium/medium-sdk-go"
)

type MediumDriver struct{}

type MediumBlogContents struct {
	HtmlBody  string
	BlogTitle string
}

func (s MediumDriver) Publish(pubCommand PublishCommand) error {
	acc, err := dal.GetPublisherAccount(pubCommand.RootPublishEvent.OwnerAccountID, pubCommand.RootPublishEvent.PublisherProfileID)
	if err != nil {
		log.Printf("correlationID: %s error loading publisher account for medium driver: %s", pubCommand.RootPublishEvent.LedgerID, err)
		return err
	}

	blogPayload, err := s.loadMediaContents(pubCommand.FinalRenderMediaRoot)
	if err != nil {
		log.Printf("correlationID: %s error downloading content for blog: %s", pubCommand.RootPublishEvent.LedgerID, err)
		return err
	}

	err = s.publishMediumArticle(pubCommand.RootPublishEvent.LedgerID, acc.PublisherAPISecretKey, blogPayload)
	if err != nil {
		log.Printf("correlationID: %s error uploading blog contents to Medium: %s", pubCommand.RootPublishEvent.LedgerID, err)
		return err
	}
	return err
}

func (s MediumDriver) loadMediaContents(mediaEvent tables.MediaEvent) (MediumBlogContents, error) {
	// TODO: allow enrichment with images.
	result := MediumBlogContents{}
	scriptPayload, err := s.loadScriptPayload(mediaEvent)
	if err != nil {
		log.Printf("correlationID: %s error initializing medium blog contents: %s", mediaEvent.LedgerID, err)
		return result, err
	}

	result.BlogTitle = scriptPayload.BlogTitle
	result.HtmlBody = scriptPayload.BlogHtml
	return result, err
}

func (s MediumDriver) loadScriptPayload(rootFinalRender tables.MediaEvent) (manifest.BlogJsonSchema, error) {
	payload, err := LoadAsString(rootFinalRender.ContentLookupKey)
	if err != nil {
		log.Printf("correlationID: %s error loading script content as string: %s", rootFinalRender.LedgerID, err)
		return manifest.BlogJsonSchema{}, err
	}
	return s.scriptPayloadToBlogJson(payload)
}

func (s MediumDriver) scriptPayloadToBlogJson(payload string) (manifest.BlogJsonSchema, error) {
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

func (s MediumDriver) publishMediumArticle(ledgerId string, apiSecret string, blogPayload MediumBlogContents) error {
	// If you have a self-issued access token, you can skip these steps and
	// create a new client directly:
	m2 := medium.NewClientWithAccessToken(apiSecret)

	// Get profile details of the user identified by the access token.
	// Empty string mean current user, otherwise you need to indicate
	// the user id (alphanumeric string with 65 chars)
	u, err := m2.GetUser("")
	if err != nil {
		log.Fatal(err)
	}

	p, err := m2.CreatePost(medium.CreatePostOptions{
		UserID:        u.ID,
		Title:         blogPayload.BlogTitle,
		Content:       blogPayload.HtmlBody,
		ContentFormat: medium.ContentFormatHTML,
		PublishStatus: medium.PublishStatusPublic,
	})
	if err != nil {
		log.Printf("correlationID: %s error publishing to Medium: %s", ledgerId, err)
		return err
	}

	// Confirm everything went ok. p.URL has the location of the created post.
	log.Println(u, p)
	return err
}
