package publisherdrivers

import (
	"errors"

	tables "github.com/bezalel-media-core/v2/dal/tables/v1"
)

type PublishCommand struct {
	RootPublishEvent tables.PublishEvent
	FinalRenderMedia tables.MediaEvent
	ScriptMedia      tables.MediaEvent // Original root
}

type PublisherDriver interface {
	Publish(PublishCommand) error
}

func GetDriver(dsitributionChannelName string) (PublisherDriver, error) {
	switch {
	case dsitributionChannelName == "Medium":
		return MediumDriver{}, nil
	case dsitributionChannelName == "Twitter":
		return TwitterDriver{}, nil
	case dsitributionChannelName == "Reddit":
		return RedditDriver{}, nil
	case dsitributionChannelName == "YouTube":
		return YouTubeDriver{}, nil
	}
	return nil, errors.New("no matching source-to-driver found")
}
