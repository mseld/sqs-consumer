package consumer

import (
	"context"

	"github.com/aws/aws-sdk-go/service/sqs"
)

// ISqsClient interface
type ISqsClient interface {
	WithSqsClient(sqs *sqs.SQS) *SqsClient
	WithQueueUrl(queueUrl string) *SqsClient
	WithBatchSize(batchSize int64) *SqsClient
	WithReceiveWaitTimeSeconds(waitSeconds int64) *SqsClient
	WithReceiveVisibilityTimeout(visibilityTimeout int64) *SqsClient
	WithTerminateVisibilityTimeout(visibilityTimeout int64) *SqsClient
	GetQueueUrl(queueName string) string
	GetQueueUrlWithContext(ctx context.Context, queueName string) string
	ReceiveMessage() ([]*sqs.Message, error)
	ReceiveMessageWithContext(ctx context.Context) ([]*sqs.Message, error)
	SendMessage(message string, delaySeconds int64) (*sqs.SendMessageOutput, error)
	SendMessageWithContext(ctx context.Context, message string, delaySeconds int64) (*sqs.SendMessageOutput, error)
	DeleteMessage(message *sqs.Message) error
	DeleteMessageWithContext(ctx context.Context, message *sqs.Message) error
	DeleteMessageBatch(messages []*sqs.Message) error
	DeleteMessageBatchWithContext(ctx context.Context, messages []*sqs.Message) error
	TerminateVisibilityTimeout(message *sqs.Message) error
	TerminateVisibilityTimeoutWithContext(ctx context.Context, message *sqs.Message) error
	TerminateVisibilityTimeoutBatch(messages []*sqs.Message) error
	TerminateVisibilityTimeoutBatchWithContext(ctx context.Context, messages []*sqs.Message) error
}

type IConsumer interface {
	Resume()
	Pause()
	Close()
	Paused() bool
	Closed() bool
	Running() bool
	Worker(h Handler)
	WorkerPool(h Handler, poolSize int)
	WithInterval(interval int) *Consumer
	WithEnableDebug(enabled bool) *Consumer
	WithBatchSize(batchSize int64) *Consumer
	WithContext(ctx context.Context) *Consumer
	WithReceiveWaitTimeSeconds(waitSeconds int64) *Consumer
	WithReceiveVisibilityTimeout(visibilityTimeout int64) *Consumer
	WithTerminateVisibilityTimeout(visibilityTimeout int64) *Consumer
}
