package consumer

import (
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
)

const (
	// BatchSize at one poll
	BatchSize int64 = 10
	// WaitTimeSeconds for each poll
	WaitTimeSeconds int64 = 20

	INFO  string = "INFO "
	WARN  string = "WARN "
	ERROR string = "ERROR"
)

// SqsClient sqs client
type SqsClient struct {
	Config *Config
	SQS    sqsiface.SQSAPI
}

type Consumer struct {
	paused   bool
	closed   bool
	poolSize int
	config   *Config
	mq       ISqsClient
}

type Config struct {
	Region            string
	QueueUrl          string
	BatchSize         int64
	WaitTimeSeconds   int64
	VisibilityTimeout int64
	PollingWaitTimeMs int
	EnableDebug       bool
}
