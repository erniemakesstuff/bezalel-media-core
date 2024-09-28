package v1

type ChannelName string

const ACCOUNT_DETAILS_RESERVED = "ReservedForAccountDetails"
const (
	Channel_Reserved_Account ChannelName = ACCOUNT_DETAILS_RESERVED
	Channel_Medium           ChannelName = "Medium"
	Channel_Twitter          ChannelName = "Twitter" // aka X
	Channel_Reddit           ChannelName = "Reddit"
	Channel_Facebook         ChannelName = "Facebook"
)

type SubscriptionStatus string

const (
	EXPIRED_BASIC   SubscriptionStatus = "ExpiredBasicSubscription"
	EXPIRED_PREMIUM SubscriptionStatus = "ExpiredPremiumSubscription"
	EXPIRED_POWER   SubscriptionStatus = "ExpiredPowerUserSubscription"
	VALID_BASIC     SubscriptionStatus = "ValidBasicSubscription"
	VALID_PREMIUM   SubscriptionStatus = "ValidPremiumSubscription"
	VALID_POWER     SubscriptionStatus = "ValidPowerUserSubscription"
	EVERGREEN_ADMIN SubscriptionStatus = "AdminSubscription" // never expire
	// TODO: Add B2B subscription profiles
)

type AccountPublisher struct {
	// Required
	AccountID               string // email, phone, social sub identity
	PublisherProfileID      string // guid. Also ACCOUNT_DETAILS_RESERVED
	ChannelName             ChannelName
	LastPublishAtEpochMilli int64

	// Optional - Account specific
	AccountSubscriptionStatus SubscriptionStatus

	// Optional - PublisherProfile specific
	PublisherAPISecretID        string
	PublisherAPISecretKey       string
	PublisherLanguage           string // ISO 639 https://en.wikipedia.org/wiki/List_of_ISO_639_language_codes
	AssignmentLockID            string // ID of the process using the lock
	AssignmentLockEpochMilliTTL int64  // Time-in-future for when lock can be forcefully released.
}
