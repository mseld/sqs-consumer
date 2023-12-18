package consumer

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

var _ Queuer = (*SqsClient)(nil)

type Queuer interface {
	SetBatchSize(batchSize int32)
	SetReceiveWaitTimeSeconds(waitSeconds int32)
	SetReceiveVisibilityTimeout(visibilityTimeout int32)
	SetTerminateVisibilityTimeout(visibilityTimeout int32)
	GetQueueUrl(ctx context.Context, queueName string) string
	ReceiveMessage(ctx context.Context) ([]types.Message, error)
	DeleteMessage(ctx context.Context, message types.Message) error
	DeleteMessageBatch(ctx context.Context, messages []types.Message) error
	TerminateVisibilityTimeout(ctx context.Context, message types.Message) error
	TerminateVisibilityTimeoutBatch(ctx context.Context, messages []types.Message) error
	SendMessage(ctx context.Context, message string, delaySeconds int32) (*sqs.SendMessageOutput, error)
}

// SqsClient represents an SQS client
type SqsClient struct {
	queueUrl                   string
	batchSize                  int32
	receiveMessageWaitSeconds  int32
	receiveVisibilityTimeout   int32
	terminateVisibilityTimeout int32
	client                     *sqs.Client
}

// NewSQSClient creates a new SQS client
func NewSQSClient(client *sqs.Client, queueUrl string) *SqsClient {
	return &SqsClient{
		queueUrl:                   queueUrl,
		batchSize:                  BatchSizeLimit,
		receiveMessageWaitSeconds:  ReceiveMessageWaitSecondsLimit,
		receiveVisibilityTimeout:   DefaultReceiveVisibilityTimeout,
		terminateVisibilityTimeout: DefaultTerminateVisibilityTimeout,
		client:                     client,
	}
}

func (s *SqsClient) SetBatchSize(batchSize int32) {
	s.batchSize = batchSize
}

func (s *SqsClient) SetReceiveWaitTimeSeconds(waitSeconds int32) {
	s.receiveMessageWaitSeconds = waitSeconds
}

func (s *SqsClient) SetReceiveVisibilityTimeout(visibilityTimeout int32) {
	s.receiveVisibilityTimeout = visibilityTimeout
}

func (s *SqsClient) SetTerminateVisibilityTimeout(visibilityTimeout int32) {
	s.terminateVisibilityTimeout = visibilityTimeout
}

// GetQueueUrl get queue url
func (s *SqsClient) GetQueueUrl(ctx context.Context, queueName string) string {
	params := &sqs.GetQueueUrlInput{
		QueueName: aws.String(queueName),
	}

	response, err := s.client.GetQueueUrl(ctx, params)
	if err != nil {
		return ""
	}

	return aws.ToString(response.QueueUrl)
}

// ReceiveMessage retrieves messages from the SQS queue
func (s *SqsClient) ReceiveMessage(ctx context.Context) ([]types.Message, error) {
	params := &sqs.ReceiveMessageInput{
		AttributeNames: []types.QueueAttributeName{
			types.QueueAttributeNameApproximateNumberOfMessages,
			types.QueueAttributeNameVisibilityTimeout,
		},
		MessageAttributeNames: []string{
			string(types.MessageSystemAttributeNameSenderId),
			string(types.MessageSystemAttributeNameSentTimestamp),
			string(types.MessageSystemAttributeNameApproximateReceiveCount),
			string(types.MessageSystemAttributeNameApproximateFirstReceiveTimestamp),
			string(types.MessageSystemAttributeNameSequenceNumber),
			string(types.MessageSystemAttributeNameMessageDeduplicationId),
			string(types.MessageSystemAttributeNameMessageGroupId),
			string(types.MessageSystemAttributeNameAWSTraceHeader),
			string(types.MessageSystemAttributeNameDeadLetterQueueSourceArn),
		},
		QueueUrl:            aws.String(s.queueUrl),
		MaxNumberOfMessages: s.batchSize,
		VisibilityTimeout:   s.receiveVisibilityTimeout,
		WaitTimeSeconds:     s.receiveMessageWaitSeconds,
	}

	result, err := s.client.ReceiveMessage(ctx, params)
	return result.Messages, err
}

// SendMessage delivers a message to the specified queue
func (s *SqsClient) SendMessage(ctx context.Context, message string, delaySeconds int32) (*sqs.SendMessageOutput, error) {
	params := &sqs.SendMessageInput{
		QueueUrl:     aws.String(s.queueUrl),
		DelaySeconds: delaySeconds,
		MessageBody:  aws.String(message),
	}

	return s.client.SendMessage(ctx, params)
}

// DeleteMessage deletes the specified message from the specified queue
func (s *SqsClient) DeleteMessage(ctx context.Context, message types.Message) error {
	params := &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(s.queueUrl),
		ReceiptHandle: message.ReceiptHandle,
	}

	_, err := s.client.DeleteMessage(ctx, params)
	return err
}

// DeleteMessageBatch deletes up to ten messages from the specified queue
func (s *SqsClient) DeleteMessageBatch(ctx context.Context, messages []types.Message) error {
	params := &sqs.DeleteMessageBatchInput{
		QueueUrl: aws.String(s.queueUrl),
	}

	for _, message := range messages {
		params.Entries = append(params.Entries, types.DeleteMessageBatchRequestEntry{
			Id:            message.MessageId,
			ReceiptHandle: message.ReceiptHandle,
		})
	}

	_, err := s.client.DeleteMessageBatch(ctx, params)
	return err
}

// TerminateVisibilityTimeout changes the visibility timeout of a specified message in a queue to a new value
func (s *SqsClient) TerminateVisibilityTimeout(ctx context.Context, message types.Message) error {
	params := &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          aws.String(s.queueUrl),
		ReceiptHandle:     message.ReceiptHandle,
		VisibilityTimeout: s.terminateVisibilityTimeout,
	}

	_, err := s.client.ChangeMessageVisibility(ctx, params)
	return err
}

// TerminateVisibilityTimeoutBatch changes the visibility timeout of multiple messages
func (s *SqsClient) TerminateVisibilityTimeoutBatch(ctx context.Context, messages []types.Message) error {
	params := &sqs.ChangeMessageVisibilityBatchInput{
		QueueUrl: aws.String(s.queueUrl),
	}

	for _, message := range messages {
		params.Entries = append(params.Entries, types.ChangeMessageVisibilityBatchRequestEntry{
			Id:                message.MessageId,
			ReceiptHandle:     message.ReceiptHandle,
			VisibilityTimeout: s.terminateVisibilityTimeout,
		})
	}

	_, err := s.client.ChangeMessageVisibilityBatch(ctx, params)
	return err
}
