package publisherdrivers

import (
	"errors"

	tables "github.com/bezalel-media-core/v2/dal/tables/v1"
)

type PublishCommand struct {
	RootPublishEvent       tables.PublishEvent
	FinalRenderMediaEvents []tables.MediaEvent
}

type PublisherDriver interface {
	Publish(PublishCommand) error
}

func GetDriver(dsitributionChannelName string) (PublisherDriver, error) {
	switch {
	case dsitributionChannelName == "Medium":
		return MediumDriver{}, nil
	}
	return nil, errors.New("no matching source-to-driver found")
}
