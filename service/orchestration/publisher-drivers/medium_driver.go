package publisherdrivers

type MediumDriver struct{}

func (s MediumDriver) Publish(pubCommand PublishCommand) error {
	return nil
}
