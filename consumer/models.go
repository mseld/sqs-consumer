package consumer

import (
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
)

// ISqsClient interface
type ISqsClient interface {
	GetQueueURL(queueName string) string
	ReceiveMessage() ([]*sqs.Message, error)
	SendMessage(message string, delaySeconds int64) (*sqs.SendMessageOutput, error)
	DeleteMessage(message *sqs.Message) error
	DeleteMessageBatch(messages []*sqs.Message) error
	TerminateVisibilityTimeout(message *sqs.Message) error
	TerminateVisibilityTimeoutBatch(messages []*sqs.Message) error
}

// SqsClient sqs client
type SqsClient struct {
	Config *Config
	SQS    sqsiface.SQSAPI
}

type Consumer struct {
	Running  bool
	Stopped  bool
	PoolSize int
	Config   *Config
	SQS      ISqsClient
}

type Config struct {
	Region            string
	QueueUrl          string
	BatchSize         int64
	WaitTimeSeconds   int64
	VisibilityTimeout int64
	PollingWaitTimeMs int
}

var (
	// BatchSize at one poll
	BatchSize int64 = 10
	// WaitTimeSeconds for each poll
	WaitTimeSeconds int64 = 20
)
