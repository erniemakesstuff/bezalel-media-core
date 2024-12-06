package publisherdrivers

import (
	"log"
	"os"
	"sync"
	"testing"

	dynamo_configuration "github.com/bezalel-media-core/v2/configuration/dynamo"
	dal "github.com/bezalel-media-core/v2/dal"
	tables "github.com/bezalel-media-core/v2/dal/tables/v1"
	manifest "github.com/bezalel-media-core/v2/manifest"
)

var once sync.Once

var PubProfile_EN_Medium_1 = tables.AccountPublisher{
	AccountID:                 "TestPublisherUser1",
	PublisherProfileID:        "MediumProfileId1",
	ChannelName:               tables.Channel_Medium,
	LastPublishAtEpochMilli:   0,
	AccountSubscriptionStatus: tables.EVERGREEN_ADMIN,
	PublisherNiche:            "TestingNiche",
	PublisherLanguage:         "EN",
	PublisherAPISecretID:      "TrueVineAIToken",
	// TODO: Inject api secret when testing.
}

func setupTest() {
	once.Do(func() {
		os.Chdir("../../..") // For manifest file loads.
		dynamo_configuration.Init()
		manifest.GetManifestLoader()
	})
}

func cleanupTestData() {
	err := dal.DeletePublisherAccount(PubProfile_EN_Medium_1.AccountID, PubProfile_EN_Medium_1.PublisherProfileID)
	if err != nil {
		log.Fatalf("failed to delete publisher account: %s", err)
	}
}

func TestMediumPublish(t *testing.T) {
	setupTest()
	pubEvent := tables.PublishEvent{
		AccountID:          PubProfile_EN_Medium_1.AccountID,
		PublisherProfileID: PubProfile_EN_Medium_1.PublisherProfileID,
	}
	cmd := PublishCommand{
		RootPublishEvent: pubEvent,
	}
	driver := MediumDriver{}
	err := driver.Publish(cmd)
	if err != nil {
		log.Printf("publisher error: %s", err)
	}
	cleanupTestData()
}

func TestTwitterPublish(t *testing.T) {
	setupTest()
	pubEvent := tables.PublishEvent{
		AccountID:          Live_EN_Twitter_1.AccountID,
		PublisherProfileID: Live_EN_Twitter_1.PublisherProfileID,
		LedgerID:           "INTEG-TestTwitterPublish",
	}
	cmd := PublishCommand{
		RootPublishEvent: pubEvent,
	}
	driver := TwitterDriver{}
	err := driver.Publish(cmd)
	if err != nil {
		log.Printf("publisher error: %s", err)
	}
	cleanupTestData()
}

func TestRedditPublish(t *testing.T) {
	setupTest()
	pubEvent := tables.PublishEvent{
		AccountID:          Live_EN_Reddit_1.AccountID,
		PublisherProfileID: Live_EN_Reddit_1.PublisherProfileID,
		LedgerID:           "INTEG-TestRedditPublish",
	}
	cmd := PublishCommand{
		RootPublishEvent: pubEvent,
	}
	driver := RedditDriver{}
	err := driver.Publish(cmd)
	if err != nil {
		log.Printf("publisher error: %s", err)
	}
	cleanupTestData()
}

func TestYouTubePublish(t *testing.T) {
	setupTest()
	pubEvent := tables.PublishEvent{
		AccountID:          Live_EN_Reddit_1.AccountID,
		PublisherProfileID: Live_EN_Reddit_1.PublisherProfileID,
		LedgerID:           "INTEG-TestYouTubePublish",
	}

	cmd := PublishCommand{
		RootPublishEvent: pubEvent,
		ScriptMedia: tables.MediaEvent{
			LedgerID:           "77fad92d-4d17-469c-aa39-62c3c55138e8",
			EventID:            "EN.Text.Drama.1de60ef338c2c6e4e88af012b82596d7",
			DistributionFormat: "ShortVideo",
			MediaType:          "Text",
			Niche:              "Drama",
			ContentLookupKey:   "Text.77fad92d-4d17-469c-aa39-62c3c55138e8.a2f17833-8380-4d73-bcb6-c55be96741eb.json",
		},
		FinalRenderMedia: tables.MediaEvent{
			LedgerID:             "77fad92d-4d17-469c-aa39-62c3c55138e8",
			EventID:              "EN.Render.Drama.d718125203c582da6a5e9c8d583b8bad",
			DistributionFormat:   "ShortVideo",
			MediaType:            "Render",
			Niche:                "Drama",
			ContentLookupKey:     "Render.77fad92d-4d17-469c-aa39-62c3c55138e8.955ad0ae-0bd5-48d2-baa2-4192c5581c7e.render",
			ParentEventID:        "EN.Text.Drama.1de60ef338c2c6e4e88af012b82596d7",
			FinalRenderSequences: "[{\"EventID\":\"EN.Text.Drama.1de60ef338c2c6e4e88af012b82596d7\",\"MediaType\":\"Text\",\"VisualPositionLayer\":\"\",\"RenderSequence\":-1,\"ContentLookupKey\":\"Text.77fad92d-4d17-469c-aa39-62c3c55138e8.a2f17833-8380-4d73-bcb6-c55be96741eb.json\"},{\"EventID\":\"EN.Image.Drama.e62048f67541d40470613616044680fe\",\"MediaType\":\"Image\",\"VisualPositionLayer\":\"Fullscreen\",\"RenderSequence\":0,\"ContentLookupKey\":\"Image.77fad92d-4d17-469c-aa39-62c3c55138e8.5ab21af0-ac38-417a-9cbd-14f619d2129e.png\"},{\"EventID\":\"EN.Music.Drama.d51dd44aed31246786008ac979766cbe\",\"MediaType\":\"Music\",\"VisualPositionLayer\":\"BackgroundMusic\",\"RenderSequence\":0,\"ContentLookupKey\":\"m1.mp3\"},{\"EventID\":\"EN.Vocal.Drama.504b21a757e0ccd6aaa93fa20bc1af26\",\"MediaType\":\"Vocal\",\"VisualPositionLayer\":\"Narrator\",\"RenderSequence\":0,\"ContentLookupKey\":\"Vocal.77fad92d-4d17-469c-aa39-62c3c55138e8.8627fe73-fe9d-4967-896b-a1fa08c82401.mp3\"},{\"EventID\":\"EN.Video.Drama.d51dd44aed31246786008ac979766cbe\",\"MediaType\":\"Video\",\"VisualPositionLayer\":\"Fullscreen\",\"RenderSequence\":1,\"ContentLookupKey\":\"b30.mp4\"},{\"EventID\":\"EN.Vocal.Drama.9a785fadebd38db53593f5e42db636f2\",\"MediaType\":\"Vocal\",\"VisualPositionLayer\":\"Narrator\",\"RenderSequence\":1,\"ContentLookupKey\":\"Vocal.77fad92d-4d17-469c-aa39-62c3c55138e8.fd3feb92-522e-4835-b100-b8403c4f4ad7.mp3\"},{\"EventID\":\"EN.Video.Drama.d51dd44aed31246786008ac979766cbe\",\"MediaType\":\"Video\",\"VisualPositionLayer\":\"Fullscreen\",\"RenderSequence\":2,\"ContentLookupKey\":\"b17.mp4\"},{\"EventID\":\"EN.Vocal.Drama.ba0e372465a87f7c7ec1f2f2609ae9f9\",\"MediaType\":\"Vocal\",\"VisualPositionLayer\":\"Narrator\",\"RenderSequence\":2,\"ContentLookupKey\":\"Vocal.77fad92d-4d17-469c-aa39-62c3c55138e8.6aebced5-3b6a-401c-b991-acc6bcdb1739.mp3\"},{\"EventID\":\"EN.Video.Drama.d51dd44aed31246786008ac979766cbe\",\"MediaType\":\"Video\",\"VisualPositionLayer\":\"Fullscreen\",\"RenderSequence\":3,\"ContentLookupKey\":\"b29.mp4\"},{\"EventID\":\"EN.Video.Drama.d51dd44aed31246786008ac979766cbe\",\"MediaType\":\"Video\",\"VisualPositionLayer\":\"Fullscreen\",\"RenderSequence\":4,\"ContentLookupKey\":\"b23.mp4\"},{\"EventID\":\"EN.Video.Drama.d51dd44aed31246786008ac979766cbe\",\"MediaType\":\"Video\",\"VisualPositionLayer\":\"Fullscreen\",\"RenderSequence\":5,\"ContentLookupKey\":\"b18.mp4\"}]",
		},
	}
	driver := YouTubeDriver{}
	err := driver.Publish(cmd)
	if err != nil {
		log.Printf("publisher error: %s", err)
	}
	cleanupTestData()
}
