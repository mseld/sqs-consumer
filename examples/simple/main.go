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
		WithReceiveWaitTimeSeconds(5).
		WithReceiveVisibilityTimeout(30).
		WithTerminateVisibilityTimeout(5).
		WithInterval(100).
		WithEnableDebug(true)

	consumerWorker.Worker(consumer.HandlerFunc(handler))

	exit := make(chan os.Signal, 1)

	signal.Notify(exit, syscall.SIGINT, syscall.SIGTERM)

	<-exit

	fmt.Println("--> Shutdown signal received")

	consumerWorker.Close()

	fmt.Println("All workers done, shutting down!")
}

func handler(ctx context.Context, record *sqs.Message) error {
	fmt.Println("Message received : ", record.MessageId, record.Body)
	return nil
}
