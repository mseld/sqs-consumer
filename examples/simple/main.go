package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/mseld/sqs-consumer/consumer"
)

func main() {
	fmt.Println("Process Running...")

	client := NewSqsClient()

	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()

	queueUrl := "https://sqs.eu-west-1.amazonaws.com/0000000000000/queue-name"

	consumerWorker := consumer.New(client, queueUrl).
		WithContext(ctx).
		WithBatchSize(10).
		WithWaitTimeSeconds(3).
		WithVisibilityTimeout(30).
		WithInterval(100).
		WithEnableDebug(true)

	consumerWorker.Worker(consumer.HandlerFunc(handler))

	termChan := make(chan os.Signal, 1)
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM)

	<-termChan
	// Handle shutdown
	fmt.Println("--> Shutdown signal received")
	// cancel()
	fmt.Println("All workers done, shutting down!")
}

func handler(ctx context.Context, record *sqs.Message) error {
	fmt.Println("Message received : ", record.MessageId, record.Body)
	return nil
}
