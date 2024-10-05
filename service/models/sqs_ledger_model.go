package models

import "time"

type S3CDC struct {
	Records []struct {
		EventVersion string    `json:"eventVersion"`
		EventSource  string    `json:"eventSource"`
		AwsRegion    string    `json:"awsRegion"`
		EventTime    time.Time `json:"eventTime"`
		EventName    string    `json:"eventName"`
		UserIdentity struct {
			PrincipalID string `json:"principalId"`
		} `json:"userIdentity"`
		RequestParameters struct {
			SourceIPAddress string `json:"sourceIPAddress"`
		} `json:"requestParameters"`
		ResponseElements struct {
			XAmzRequestID string `json:"x-amz-request-id"`
			XAmzID2       string `json:"x-amz-id-2"`
		} `json:"responseElements"`
		S3 struct {
			S3SchemaVersion string `json:"s3SchemaVersion"`
			ConfigurationID string `json:"configurationId"`
			Bucket          struct {
				Name          string `json:"name"`
				OwnerIdentity struct {
					PrincipalID string `json:"principalId"`
				} `json:"ownerIdentity"`
				Arn string `json:"arn"`
			} `json:"bucket"`
			Object struct {
				Key       string `json:"key"`
				Size      int    `json:"size"`
				ETag      string `json:"eTag"`
				Sequencer string `json:"sequencer"`
			} `json:"object"`
		} `json:"s3"`
	} `json:"Records"`
}

type DynamoCDC struct {
	EventID      string `json:"eventID"`
	EventName    string `json:"eventName"`
	EventVersion string `json:"eventVersion"`
	EventSource  string `json:"eventSource"`
	AwsRegion    string `json:"awsRegion"`
	Dynamodb     struct {
		ApproximateCreationDateTime int `json:"ApproximateCreationDateTime"`
		Keys                        struct {
			LedgerID struct {
				S string `json:"S"`
			} `json:"LedgerID"`
		} `json:"Keys"`
		NewImage struct {
			MediaEventsVersion struct {
				N string `json:"N"`
			} `json:"MediaEventsVersion"`
			RawEventSource struct {
				S string `json:"S"`
			} `json:"RawEventSource"`
			ScriptEventsVersion struct {
				N string `json:"N"`
			} `json:"ScriptEventsVersion"`
			RawContentHash struct {
				S string `json:"S"`
			} `json:"RawContentHash"`
			LedgerID struct {
				S string `json:"S"`
			} `json:"LedgerID"`
			RawEventLanguage struct {
				S string `json:"S"`
			} `json:"RawEventLanguage"`
			PublishEvents struct {
				Null bool   `json:"NULL"`
				S    string `json:"S"`
			} `json:"PublishEvents"`
			RawEventMediaUrls struct {
				Null bool   `json:"NULL"`
				S    string `json:"S"`
			} `json:"RawEventMediaUrls"`
			PublishEventsVersion struct {
				N string `json:"N"`
			} `json:"PublishEventsVersion"`
			ScriptEvents struct {
				Null bool   `json:"NULL"`
				S    string `json:"S"`
			} `json:"ScriptEvents"`
			RawEventWebsiteUrls struct {
				Null bool   `json:"NULL"`
				S    string `json:"S"`
			} `json:"RawEventWebsiteUrls"`
			LedgerStatus struct {
				S string `json:"S"`
			} `json:"LedgerStatus"`
			MediaEvents struct {
				Null bool   `json:"NULL"`
				S    string `json:"S"`
			} `json:"MediaEvents"`
			LedgerCreatedAtEpochMilli struct {
				N string `json:"N"`
			} `json:"LedgerCreatedAtEpochMilli"`
			RawEventPayload struct {
				S string `json:"S"`
			} `json:"RawEventPayload"`
		} `json:"NewImage"`
		SequenceNumber string `json:"SequenceNumber"`
		SizeBytes      int    `json:"SizeBytes"`
		StreamViewType string `json:"StreamViewType"`
	} `json:"dynamodb"`
	EventSourceARN string `json:"eventSourceARN"`
}

type SQSMessage struct {
	Type             string    `json:"Type"`
	MessageID        string    `json:"MessageId"`
	TopicArn         string    `json:"TopicArn"`
	Message          string    `json:"Message"`
	Timestamp        time.Time `json:"Timestamp"`
	SignatureVersion string    `json:"SignatureVersion"`
	Signature        string    `json:"Signature"`
	SigningCertURL   string    `json:"SigningCertURL"`
	UnsubscribeURL   string    `json:"UnsubscribeURL"`
}
