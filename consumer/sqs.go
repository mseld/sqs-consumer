package consumer

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
)

// NewSQSClient create new sqs client
func NewSQSClient(sqs *sqs.SQS, config *Config) ISqsClient {
	client := &SqsClient{
		Config: config,
		SQS:    sqs,
	}

	return client
}

// GetQueueURL get queue url
func (client *SqsClient) GetQueueURL(queueName string) string {
	params := &sqs.GetQueueUrlInput{
		QueueName: aws.String(queueName),
	}

	response, err := client.SQS.GetQueueUrl(params)

	if err != nil {
		return ""
	}

	return aws.StringValue(response.QueueUrl)
}

// ReceiveMessage retrive message from sqs queue
func (client *SqsClient) ReceiveMessage() ([]*sqs.Message, error) {
	params := &sqs.ReceiveMessageInput{
		AttributeNames: []*string{
			aws.String(sqs.MessageSystemAttributeNameSentTimestamp),
			aws.String(sqs.MessageSystemAttributeNameApproximateReceiveCount),
		},
		MessageAttributeNames: []*string{
			aws.String(sqs.QueueAttributeNameAll),
		},
		QueueUrl:            aws.String(client.Config.QueueUrl),
		MaxNumberOfMessages: aws.Int64(client.Config.BatchSize),
		VisibilityTimeout:   aws.Int64(client.Config.VisibilityTimeout),
		WaitTimeSeconds:     aws.Int64(client.Config.WaitTimeSeconds),
	}

	result, err := client.SQS.ReceiveMessage(params)

	return result.Messages, err
}

// SendMessage send message to sqs queue
func (client *SqsClient) SendMessage(message string, delaySeconds int64) (*sqs.SendMessageOutput, error) {
	return client.SQS.SendMessage(&sqs.SendMessageInput{
		QueueUrl:     aws.String(client.Config.QueueUrl),
		DelaySeconds: aws.Int64(delaySeconds),
		MessageBody:  aws.String(message),
	})
}

// DeleteMessage delete message from sqs queue
func (client *SqsClient) DeleteMessage(message *sqs.Message) error {
	params := &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(client.Config.QueueUrl),
		ReceiptHandle: message.ReceiptHandle,
	}

	_, err := client.SQS.DeleteMessage(params)

	return err
}

// DeleteMessageBatch delete messages from sqs queue
func (client *SqsClient) DeleteMessageBatch(messages []*sqs.Message) error {
	params := &sqs.DeleteMessageBatchInput{
		QueueUrl: aws.String(client.Config.QueueUrl),
	}

	for _, message := range messages {
		params.Entries = append(params.Entries, &sqs.DeleteMessageBatchRequestEntry{
			Id:            message.MessageId,
			ReceiptHandle: message.ReceiptHandle,
		})
	}

	_, err := client.SQS.DeleteMessageBatch(params)

	return err
}

// TerminateVisibilityTimeout make message visible to be processed from another worker
func (client *SqsClient) TerminateVisibilityTimeout(message *sqs.Message) error {
	params := &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          aws.String(client.Config.QueueUrl),
		ReceiptHandle:     message.ReceiptHandle,
		VisibilityTimeout: aws.Int64(0),
	}

	_, err := client.SQS.ChangeMessageVisibility(params)

	return err
}

// TerminateVisibilityTimeoutBatch make messages visible to be processed from another worker
func (client *SqsClient) TerminateVisibilityTimeoutBatch(messages []*sqs.Message) error {
	params := &sqs.ChangeMessageVisibilityBatchInput{
		QueueUrl: aws.String(client.Config.QueueUrl),
	}

	for _, message := range messages {
		params.Entries = append(params.Entries, &sqs.ChangeMessageVisibilityBatchRequestEntry{
			Id:                message.MessageId,
			ReceiptHandle:     message.ReceiptHandle,
			VisibilityTimeout: aws.Int64(0),
		})
	}

	_, err := client.SQS.ChangeMessageVisibilityBatch(params)

	return err
}
