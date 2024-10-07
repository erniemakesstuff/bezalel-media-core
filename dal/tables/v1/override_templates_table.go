package v1

type TemplateType string

const (
	TT_DESCRIPTION TemplateType = "DescriptionTemplate"
	TT_AVATAR      TemplateType = "AvatarTemplate"
)

type OverrideTemplate struct {
	// Required
	AccountID  string // Owner account ID.
	TemplateID string // some guid

	// Optional
	TargetContentAssociation  TemplateType // Where or how to apply the template.
	IsFullReplacement         bool         // Appends-only when false (default). Replaces when true.
	DistributionChannelScopes string       // [YouTube, Instagram, ...] Where template can apply
}
