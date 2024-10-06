package dynamo

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	aws_configuration "github.com/bezalel-media-core/v2/configuration"

	"log"
)

const TABLE_ACCOUNTS = "Accounts"
const TABLE_EVENT_LEDGER = "EventLedger"

// Although status is derivable from ledger data, needed for index-lookup replayability.
const EVENT_LEDGER_STATE_GSI_NAME = "LedgerStatusIndex"   // {Status, StartedAtEpochMilli}
const PUBLISHER_PROFILE_GSI_NAME = "ChannelPlatformIndex" // For querying by YouTube, Instagram, ...
const MAX_QPS_ON_DEMAND_GSI = 50

func Init() {
	log.Printf("Initializing DynamoDB Tables")

	// Create DynamoDB client
	svc := dynamodb.New(aws_configuration.GetAwsSession())
	createTableAccounts(svc)
	createEventLedgerTables(svc)
}

// Creates Accounts Table + PublisherProfile details.
// PK: AccountID (should ideally be some sub-identity guid to avoid hot-partitions; otherwise email, phone, etc.)
// PartitionSalt: GUID, ensure partition distribution within a shard.
// SubscriptionStatus: Expired, Free, Premium, PowerUser,... (Expired == Free)
// Downstream reads to collect PublisherProfiles per-account will need to
// be a quorum read across all N-publisherProfile shards.
// SK: <PublisherProfileID> - GUID
// LastPublishAtEpochMilli - time.Now().UnixMilli()
// Proviisions GSI for querying by PlatformChannel and LastPublishAtEpochMilli
// Filter by ChannelTheme, and ChannelLanguage.
// Contains Custom profile avatar prompts, and descriptions.
// RANGE: DEFAULT - contains prompting templates to assign to other publisher-profiles in-range.
func createTableAccounts(svc *dynamodb.DynamoDB) {
	tableName := TABLE_ACCOUNTS
	input := &dynamodb.CreateTableInput{
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("AccountID"),
				AttributeType: aws.String("S"),
			},
			{
				AttributeName: aws.String("PublisherProfileID"),
				AttributeType: aws.String("S"),
			},
			{
				AttributeName: aws.String("LastPublishAtEpochMilli"),
				AttributeType: aws.String("N"),
			},
			{
				AttributeName: aws.String("ChannelName"),
				AttributeType: aws.String("S"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("AccountID"),
				KeyType:       aws.String("HASH"),
			},
			{
				AttributeName: aws.String("PublisherProfileID"),
				KeyType:       aws.String("RANGE"),
			},
		},
		GlobalSecondaryIndexes: []*dynamodb.GlobalSecondaryIndex{
			{
				IndexName: aws.String(PUBLISHER_PROFILE_GSI_NAME),
				KeySchema: []*dynamodb.KeySchemaElement{
					{
						AttributeName: aws.String("ChannelName"),
						KeyType:       aws.String("HASH"),
					},
					{ // Use TTL instead of LastPublish because you don't want to select records that are locked on.
						AttributeName: aws.String("LastPublishAtEpochMilli"),
						KeyType:       aws.String("RANGE"),
					},
				},
				Projection: &dynamodb.Projection{
					ProjectionType: aws.String(dynamodb.ProjectionTypeAll),
				},
				OnDemandThroughput: &dynamodb.OnDemandThroughput{
					MaxReadRequestUnits:  aws.Int64(MAX_QPS_ON_DEMAND_GSI),
					MaxWriteRequestUnits: aws.Int64(MAX_QPS_ON_DEMAND_GSI),
				},
			},
		},
		BillingMode: aws.String(dynamodb.BillingModePayPerRequest),
		TableName:   aws.String(tableName),
	}
	createTable(svc, input, tableName)
}

func createEventLedgerTables(svc *dynamodb.DynamoDB) {
	tableName := TABLE_EVENT_LEDGER
	input := &dynamodb.CreateTableInput{
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("LedgerID"),
				AttributeType: aws.String("S"),
			},
			{
				AttributeName: aws.String("LedgerStatus"),
				AttributeType: aws.String("S"),
			},
			{
				AttributeName: aws.String("LedgerCreatedAtEpochMilli"),
				AttributeType: aws.String("N"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("LedgerID"),
				KeyType:       aws.String("HASH"),
			},
		},
		GlobalSecondaryIndexes: []*dynamodb.GlobalSecondaryIndex{
			{
				IndexName: aws.String(EVENT_LEDGER_STATE_GSI_NAME),
				KeySchema: []*dynamodb.KeySchemaElement{
					{
						AttributeName: aws.String("LedgerStatus"),
						KeyType:       aws.String("HASH"),
					},
					{
						AttributeName: aws.String("LedgerCreatedAtEpochMilli"),
						KeyType:       aws.String("RANGE"),
					},
				},
				Projection: &dynamodb.Projection{
					ProjectionType: aws.String(dynamodb.ProjectionTypeAll),
				},
				OnDemandThroughput: &dynamodb.OnDemandThroughput{
					MaxReadRequestUnits:  aws.Int64(MAX_QPS_ON_DEMAND_GSI),
					MaxWriteRequestUnits: aws.Int64(MAX_QPS_ON_DEMAND_GSI),
				},
			},
		},
		BillingMode: aws.String(dynamodb.BillingModePayPerRequest),
		TableName:   aws.String(tableName),
		StreamSpecification: &dynamodb.StreamSpecification{
			StreamEnabled:  aws.Bool(true),
			StreamViewType: aws.String(dynamodb.StreamViewTypeKeysOnly),
		},
	}
	createTable(svc, input, tableName)
}

func createTable(svc *dynamodb.DynamoDB, input *dynamodb.CreateTableInput, tableName string) {
	_, err := svc.CreateTable(input)
	if tableAlreadyExists(err) {
		log.Println("Table already exists", tableName)
	} else if err != nil {
		log.Fatalf("Got error calling CreateTable: %s", err)
	} else {
		log.Println("Created the table", tableName)
	}
}

func tableAlreadyExists(err error) bool {
	if err == nil {
		return false
	}
	if aerr, ok := err.(awserr.Error); ok {
		return aerr.Code() == dynamodb.ErrCodeResourceInUseException
	}
	return false
}
