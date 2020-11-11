package main

import (
	"fmt"
	"runtime"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/mseld/sqs-consumer/consumer"
)

func main() {
	fmt.Println("Process Running...")

	config := aws.NewConfig().WithRegion("eu-west-1").WithMaxRetries(3)
	awsSession := session.Must(session.NewSession())
	sqsInstance := sqs.New(awsSession, config)

	// ctx, cancel := context.WithCancel(context.Background())
	// defer cancel()

	sqsConsumer := consumer.New(sqsInstance, &consumer.Config{
		Region:            "eu-west-1",
		QueueUrl:          "https://sqs.eu-west-1.amazonaws.com/763224933484/sam-test",
		BatchSize:         10,
		WaitTimeSeconds:   10,
		VisibilityTimeout: 30,
		PollingWaitTimeMs: 100,
	})

	sqsConsumer.Worker(consumer.HandlerFunc(handler))
	sqsConsumer.Start()
	runtime.Goexit()
}

func handler(record *sqs.Message) error {
	fmt.Println("Received a new message : ", record.MessageId, record)

	fmt.Println("Do stuff...")

	return nil
}
