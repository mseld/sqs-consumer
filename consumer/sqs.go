package consumer

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
)

// SqsClient sqs client
type SqsClient struct {
	QueueUrl          string
	BatchSize         int64
	WaitTimeSeconds   int64
	VisibilityTimeout int64
	SQS               sqsiface.SQSAPI
}

// NewSQSClient create new sqs client
func NewSQSClient(sqs *sqs.SQS, queueUrl string) ISqsClient {
	client := &SqsClient{
		QueueUrl:          queueUrl,
		BatchSize:         10,
		WaitTimeSeconds:   20,
		VisibilityTimeout: 30,
		SQS:               sqs,
	}

	return client
}

func (client *SqsClient) WithSqsClient(sqs *sqs.SQS) *SqsClient {
	client.SQS = sqs
	return client
}

func (client *SqsClient) WithQueueUrl(queueUrl string) *SqsClient {
	client.QueueUrl = queueUrl
	return client
}

func (client *SqsClient) WithBatchSize(batchSize int64) *SqsClient {
	client.BatchSize = batchSize
	return client
}

func (client *SqsClient) WithWaitTimeSeconds(waitTimeSeconds int64) *SqsClient {
	client.WaitTimeSeconds = waitTimeSeconds
	return client
}

func (client *SqsClient) WithVisibilityTimeout(visibilityTimeout int64) *SqsClient {
	client.VisibilityTimeout = visibilityTimeout
	return client
}

// GetQueueUrl get queue url
func (client *SqsClient) GetQueueUrl(queueName string) string {
	return client.GetQueueUrlWithContext(context.TODO(), queueName)
}

// GetQueueUrlWithContext get queue url
func (client *SqsClient) GetQueueUrlWithContext(ctx context.Context, queueName string) string {
	params := &sqs.GetQueueUrlInput{
		QueueName: aws.String(queueName),
	}

	response, err := client.SQS.GetQueueUrlWithContext(ctx, params)
	if err != nil {
		return ""
	}

	return aws.StringValue(response.QueueUrl)
}

// ReceiveMessage retrive message from sqs queue
func (client *SqsClient) ReceiveMessage() ([]*sqs.Message, error) {
	return client.ReceiveMessageWithContext(context.TODO())
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
		QueueUrl:            aws.String(client.QueueUrl),
		MaxNumberOfMessages: aws.Int64(client.BatchSize),
		VisibilityTimeout:   aws.Int64(client.VisibilityTimeout),
		WaitTimeSeconds:     aws.Int64(client.WaitTimeSeconds),
	}

	result, err := client.SQS.ReceiveMessageWithContext(ctx, params)
	return result.Messages, err
}

// SendMessage send message to sqs queue
func (client *SqsClient) SendMessage(message string, delaySeconds int64) (*sqs.SendMessageOutput, error) {
	return client.SendMessageWithContext(context.TODO(), message, delaySeconds)
}

// SendMessageWithContext send message to sqs queue
func (client *SqsClient) SendMessageWithContext(ctx context.Context, message string, delaySeconds int64) (*sqs.SendMessageOutput, error) {
	params := &sqs.SendMessageInput{
		QueueUrl:     aws.String(client.QueueUrl),
		DelaySeconds: aws.Int64(delaySeconds),
		MessageBody:  aws.String(message),
	}
	return client.SQS.SendMessageWithContext(ctx, params)
}

// DeleteMessage delete message from sqs queue
func (client *SqsClient) DeleteMessage(message *sqs.Message) error {
	return client.DeleteMessageWithContext(context.TODO(), message)
}

// DeleteMessageWithContext delete message from sqs queue
func (client *SqsClient) DeleteMessageWithContext(ctx context.Context, message *sqs.Message) error {
	params := &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(client.QueueUrl),
		ReceiptHandle: message.ReceiptHandle,
	}

	_, err := client.SQS.DeleteMessageWithContext(ctx, params)
	return err
}

// DeleteMessageBatch delete messages from sqs queue
func (client *SqsClient) DeleteMessageBatch(messages []*sqs.Message) error {
	return client.DeleteMessageBatchWithContext(context.TODO(), messages)
}

// DeleteMessageBatchWithContext delete messages from sqs queue
func (client *SqsClient) DeleteMessageBatchWithContext(ctx context.Context, messages []*sqs.Message) error {
	params := &sqs.DeleteMessageBatchInput{
		QueueUrl: aws.String(client.QueueUrl),
	}

	for _, message := range messages {
		params.Entries = append(params.Entries, &sqs.DeleteMessageBatchRequestEntry{
			Id:            message.MessageId,
			ReceiptHandle: message.ReceiptHandle,
		})
	}

	_, err := client.SQS.DeleteMessageBatchWithContext(ctx, params)
	return err
}

// TerminateVisibilityTimeout make message visible to be processed from another worker
func (client *SqsClient) TerminateVisibilityTimeout(message *sqs.Message) error {
	return client.TerminateVisibilityTimeoutWithContext(context.TODO(), message)
}

// TerminateVisibilityTimeoutWithContext make message visible to be processed from another worker
func (client *SqsClient) TerminateVisibilityTimeoutWithContext(ctx context.Context, message *sqs.Message) error {
	params := &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          aws.String(client.QueueUrl),
		ReceiptHandle:     message.ReceiptHandle,
		VisibilityTimeout: aws.Int64(0),
	}

	_, err := client.SQS.ChangeMessageVisibilityWithContext(ctx, params)
	return err
}

// TerminateVisibilityTimeoutBatch make messages visible to be processed from another worker
func (client *SqsClient) TerminateVisibilityTimeoutBatch(messages []*sqs.Message) error {
	return client.TerminateVisibilityTimeoutBatchWithContext(context.TODO(), messages)
}

// TerminateVisibilityTimeoutBatchWithContext make messages visible to be processed from another worker
func (client *SqsClient) TerminateVisibilityTimeoutBatchWithContext(ctx context.Context, messages []*sqs.Message) error {
	params := &sqs.ChangeMessageVisibilityBatchInput{
		QueueUrl: aws.String(client.QueueUrl),
	}

	for _, message := range messages {
		params.Entries = append(params.Entries, &sqs.ChangeMessageVisibilityBatchRequestEntry{
			Id:                message.MessageId,
			ReceiptHandle:     message.ReceiptHandle,
			VisibilityTimeout: aws.Int64(0),
		})
	}

	_, err := client.SQS.ChangeMessageVisibilityBatchWithContext(ctx, params)
	return err
}
