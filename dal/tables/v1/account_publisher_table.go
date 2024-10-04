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

// Filter expresision Expired*
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
	PreferredLanguage         string

	// Optional - PublisherProfile specific
	PublisherAPISecretID  string
	PublisherAPISecretKey string
	PublisherLanguage     string // ISO 639 https://en.wikipedia.org/wiki/List_of_ISO_639_language_codes
	PublisherNiche        string // Drama, news, ...
	AssignmentLockID      string // ID of the process using the lock for assignment and media rendering.
	AssignmentLockTTL     int64  // Time-in-future for when lock can be forcefully released for re-assignement. Epoch Milliseconds.
	PublishLockID         string // ID of the process performing the publish to distribution channels.
	PublishLockTTL        int64  // Epoch Milliseconds.
}
