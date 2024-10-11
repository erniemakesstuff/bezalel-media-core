package publisherdrivers

type MediumDriver struct{}

func (s MediumDriver) Publish(pubCommand PublishCommand) error {
	// TODO: fetch client secrets based on publish command PK/SK.
	return nil
}
