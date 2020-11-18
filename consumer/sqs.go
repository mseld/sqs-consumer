package consumer

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
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

// SqsClient sqs client
type SqsClient struct {
	queueUrl                   string
	batchSize                  int64
	receiveMessageWaitSeconds  int64
	receiveVisibilityTimeout   int64
	terminateVisibilityTimeout int64
	sqs                        sqsiface.SQSAPI
}

// NewSQSClient create new sqs client
func NewSQSClient(sqs *sqs.SQS, queueUrl string) ISqsClient {
	client := &SqsClient{
		queueUrl:                   queueUrl,
		batchSize:                  BatchSizeLimit,
		receiveMessageWaitSeconds:  ReceiveMessageWaitSecondsLimit,
		receiveVisibilityTimeout:   DefaultReceiveVisibilityTimeout,
		terminateVisibilityTimeout: DefaultTerminateVisibilityTimeout,
		sqs:                        sqs,
	}

	return client
}

func (client *SqsClient) WithSqsClient(sqs *sqs.SQS) *SqsClient {
	client.sqs = sqs
	return client
}

func (client *SqsClient) WithQueueUrl(queueUrl string) *SqsClient {
	client.queueUrl = queueUrl
	return client
}

func (client *SqsClient) WithBatchSize(batchSize int64) *SqsClient {
	client.batchSize = batchSize
	return client
}

func (client *SqsClient) WithReceiveWaitTimeSeconds(waitSeconds int64) *SqsClient {
	client.receiveMessageWaitSeconds = waitSeconds
	return client
}

func (client *SqsClient) WithReceiveVisibilityTimeout(visibilityTimeout int64) *SqsClient {
	client.receiveVisibilityTimeout = visibilityTimeout
	return client
}

func (client *SqsClient) WithTerminateVisibilityTimeout(visibilityTimeout int64) *SqsClient {
	client.terminateVisibilityTimeout = visibilityTimeout
	return client
}

// GetQueueUrl get queue url
func (client *SqsClient) GetQueueUrl(queueName string) string {
	return client.GetQueueUrlWithContext(context.Background(), queueName)
}

// GetQueueUrlWithContext get queue url
func (client *SqsClient) GetQueueUrlWithContext(ctx context.Context, queueName string) string {
	params := &sqs.GetQueueUrlInput{
		QueueName: aws.String(queueName),
	}

	response, err := client.sqs.GetQueueUrlWithContext(ctx, params)
	if err != nil {
		return ""
	}

	return aws.StringValue(response.QueueUrl)
}

// ReceiveMessage retrive message from sqs queue
func (client *SqsClient) ReceiveMessage() ([]*sqs.Message, error) {
	return client.ReceiveMessageWithContext(context.Background())
}

// ReceiveMessageWithContext retrive message from sqs queue
func (client *SqsClient) ReceiveMessageWithContext(ctx context.Context) ([]*sqs.Message, error) {
	params := &sqs.ReceiveMessageInput{
		AttributeNames: []*string{
			aws.String(sqs.MessageSystemAttributeNameSentTimestamp),
			aws.String(sqs.MessageSystemAttributeNameApproximateReceiveCount),
		},
		MessageAttributeNames: []*string{
			aws.String(sqs.QueueAttributeNameAll),
		},
		QueueUrl:            aws.String(client.queueUrl),
		MaxNumberOfMessages: aws.Int64(client.batchSize),
		VisibilityTimeout:   aws.Int64(client.receiveVisibilityTimeout),
		WaitTimeSeconds:     aws.Int64(client.receiveMessageWaitSeconds),
	}

	result, err := client.sqs.ReceiveMessageWithContext(ctx, params)
	return result.Messages, err
}

// SendMessage send message to sqs queue
func (client *SqsClient) SendMessage(message string, delaySeconds int64) (*sqs.SendMessageOutput, error) {
	return client.SendMessageWithContext(context.Background(), message, delaySeconds)
}

// SendMessageWithContext send message to sqs queue
func (client *SqsClient) SendMessageWithContext(ctx context.Context, message string, delaySeconds int64) (*sqs.SendMessageOutput, error) {
	params := &sqs.SendMessageInput{
		QueueUrl:     aws.String(client.queueUrl),
		DelaySeconds: aws.Int64(delaySeconds),
		MessageBody:  aws.String(message),
	}
	return client.sqs.SendMessageWithContext(ctx, params)
}

// DeleteMessage delete message from sqs queue
func (client *SqsClient) DeleteMessage(message *sqs.Message) error {
	return client.DeleteMessageWithContext(context.Background(), message)
}

// DeleteMessageWithContext delete message from sqs queue
func (client *SqsClient) DeleteMessageWithContext(ctx context.Context, message *sqs.Message) error {
	params := &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(client.queueUrl),
		ReceiptHandle: message.ReceiptHandle,
	}

	_, err := client.sqs.DeleteMessageWithContext(ctx, params)
	return err
}

// DeleteMessageBatch delete messages from sqs queue
func (client *SqsClient) DeleteMessageBatch(messages []*sqs.Message) error {
	return client.DeleteMessageBatchWithContext(context.Background(), messages)
}

// DeleteMessageBatchWithContext delete messages from sqs queue
func (client *SqsClient) DeleteMessageBatchWithContext(ctx context.Context, messages []*sqs.Message) error {
	params := &sqs.DeleteMessageBatchInput{
		QueueUrl: aws.String(client.queueUrl),
	}

	for _, message := range messages {
		params.Entries = append(params.Entries, &sqs.DeleteMessageBatchRequestEntry{
			Id:            message.MessageId,
			ReceiptHandle: message.ReceiptHandle,
		})
	}

	_, err := client.sqs.DeleteMessageBatchWithContext(ctx, params)
	return err
}

// TerminateVisibilityTimeout make message visible to be processed from another worker
func (client *SqsClient) TerminateVisibilityTimeout(message *sqs.Message) error {
	return client.TerminateVisibilityTimeoutWithContext(context.Background(), message)
}

// TerminateVisibilityTimeoutWithContext make message visible to be processed from another worker
func (client *SqsClient) TerminateVisibilityTimeoutWithContext(ctx context.Context, message *sqs.Message) error {
	params := &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          aws.String(client.queueUrl),
		ReceiptHandle:     message.ReceiptHandle,
		VisibilityTimeout: aws.Int64(client.terminateVisibilityTimeout),
	}

	_, err := client.sqs.ChangeMessageVisibilityWithContext(ctx, params)
	return err
}

// TerminateVisibilityTimeoutBatch make messages visible to be processed from another worker
func (client *SqsClient) TerminateVisibilityTimeoutBatch(messages []*sqs.Message) error {
	return client.TerminateVisibilityTimeoutBatchWithContext(context.Background(), messages)
}

// TerminateVisibilityTimeoutBatchWithContext make messages visible to be processed from another worker
func (client *SqsClient) TerminateVisibilityTimeoutBatchWithContext(ctx context.Context, messages []*sqs.Message) error {
	params := &sqs.ChangeMessageVisibilityBatchInput{
		QueueUrl: aws.String(client.queueUrl),
	}

	for _, message := range messages {
		params.Entries = append(params.Entries, &sqs.ChangeMessageVisibilityBatchRequestEntry{
			Id:                message.MessageId,
			ReceiptHandle:     message.ReceiptHandle,
			VisibilityTimeout: aws.Int64(0),
		})
	}

	_, err := client.sqs.ChangeMessageVisibilityBatchWithContext(ctx, params)
	return err
}
