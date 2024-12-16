package publisherdrivers

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/ChimeraCoder/anaconda"
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
	Images        []string
}

func (s TwitterDriver) Publish(pubCommand PublishCommand) error {
	acc, err := dal.GetPublisherAccount(pubCommand.RootPublishEvent.AccountID, pubCommand.RootPublishEvent.PublisherProfileID)
	if err != nil {
		log.Printf("correlationID: %s error loading publisher account for Twitter driver: %s", pubCommand.RootPublishEvent.LedgerID, err)
		return err
	}
	blogPayload, err := s.loadMediaContents(pubCommand.FinalRenderMedia)
	if err != nil {
		log.Printf("correlationID: %s error downloading content for tinyblog: %s", pubCommand.RootPublishEvent.LedgerID, err)
		return err
	}
	err = s.publishTwitterPost(pubCommand.RootPublishEvent.LedgerID, acc, blogPayload)
	if err != nil {
		log.Printf("correlationID: %s error uploading blog contents to Twitter: %s", pubCommand.RootPublishEvent.LedgerID, err)
		return err
	}
	return err
}

func (s TwitterDriver) loadMediaContents(mediaEvent tables.MediaEvent) (TwitterPostContents, error) {
	// TODO: allow enrichment with images.
	result := TwitterPostContents{}
	var err error
	scriptPayload, err := s.loadScriptPayload(mediaEvent)
	if err != nil {
		log.Printf("correlationID: %s error initializing twitter blog contents: %s", mediaEvent.LedgerID, err)
		return result, err
	}

	renderSequences := []tables.RenderMediaSequence{}
	err = json.Unmarshal([]byte(mediaEvent.FinalRenderSequences), &renderSequences)
	if err != nil {
		log.Printf("correlationID: %s error deserializing renderSequences for twitter: %s", mediaEvent.LedgerID, err)
		return result, err
	}

	result.TweetTextBody = scriptPayload.BlogText
	result.Images = []string{}
	for _, r := range renderSequences {
		if r.MediaType == tables.MEDIA_IMAGE {
			result.Images = append(result.Images, r.ContentLookupKey)
		}
	}
	return result, err
}

func (s TwitterDriver) loadScriptPayload(rootFinalRender tables.MediaEvent) (manifest.TinyBlogSchema, error) {
	payload, err := LoadAsString(rootFinalRender.ContentLookupKey)
	if err != nil {
		log.Printf("correlationID: %s error loading script content as string: %s", rootFinalRender.LedgerID, err)
		return manifest.TinyBlogSchema{}, err
	}
	return ScriptPayloadToTinyBlogSchema(payload)
}

func (s TwitterDriver) publishTwitterPost(ledgerId string, account tables.AccountPublisher, tweetPayload TwitterPostContents) error {
	mediaIds, err := s.uploadImages(account, tweetPayload)
	if err != nil {
		log.Printf("correlationID: %s error uploading twitter images: %s", ledgerId, err)
	}

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

	if len(mediaIds) != 0 {
		p.Media = &types.CreateInputMedia{
			MediaIDs: mediaIds,
		}
	}

	res, err := managetweet.Create(context.Background(), c, p)
	if err != nil {
		return s.setAnyBadRequestCode(err)
	}

	// You can access tweet by x.com/anyuser/status/<TweetId>
	log.Printf("correlationID: %s tweeted: %s", ledgerId, gotwi.StringValue(res.Data.ID))
	return err
}

func (s TwitterDriver) setAnyBadRequestCode(err error) error {
	isCredentialError := strings.Contains(fmt.Sprintf("%s", err), "httpStatusCode=403") ||
		strings.Contains(fmt.Sprintf("%s", err), "httpStatusCode=401")
	if isCredentialError {
		return fmt.Errorf("%s: Twitter profile resulted in bad request: %s", BAD_REQUEST_PROFILE_CODE, err)
	}
	return err
}

// https://developer.x.com/en/docs/tutorials/uploading-media
// Returns mediaIds.
func (s TwitterDriver) uploadImages(account tables.AccountPublisher, tweetPayload TwitterPostContents) ([]string, error) {
	mediaIds := []string{}
	api := anaconda.NewTwitterApiWithCredentials(account.UserAccessToken, account.UserAccessTokenSecret,
		account.PublisherAPISecretID, account.PublisherAPISecretKey)

	for _, imageS3Url := range tweetPayload.Images {
		imageBytes, err := LoadAsBytes(imageS3Url)
		if err != nil {
			return mediaIds, err
		}
		base64String := base64.StdEncoding.EncodeToString(imageBytes)
		uploadResp, err := api.UploadMedia(base64String)
		if err != nil {
			return mediaIds, err
		}
		mediaIds = append(mediaIds, uploadResp.MediaIDString)

		// Avoid rate limiting.
		time.Sleep(5 * time.Second)
	}

	return mediaIds, nil
}
