package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

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

	_, cancel := context.WithCancel(context.Background())
	// defer cancel()

	Consumer := consumer.New(sqsInstance, &consumer.Config{
		Region:            "eu-west-1",
		QueueUrl:          "https://sqs.eu-west-1.amazonaws.com/763224933484/sam-test",
		BatchSize:         10,
		WaitTimeSeconds:   10,
		VisibilityTimeout: 30,
		PollingWaitTimeMs: 1000,
		EnableDebug:       true,
	})

	Consumer.Worker(consumer.HandlerFunc(handler))

	Consumer.Start()

	termChan := make(chan os.Signal, 1)
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM)

	<-termChan
	// Handle shutdown
	fmt.Println("--> Shutdown signal received")
	cancel()

	fmt.Println("All workers done, shutting down!")
	// runtime.Goexit()
}

func handler(record *sqs.Message) error {
	fmt.Println("Received a new message : ", record.MessageId, record)

	fmt.Println("Do stuff...")

	return nil
}
