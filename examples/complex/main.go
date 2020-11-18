package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/mseld/sqs-consumer/consumer"
	"github.com/mseld/sqs-consumer/examples/utils"
)

func main() {
	log.Println("Worker Started")

	client := utils.NewSqsClient()

	ctx, cancel := context.WithCancel(context.Background())

	wg := &sync.WaitGroup{}

	queueUrl := "https://sqs.eu-west-1.amazonaws.com/0000000000000/queue-name"

	consumerWorker := consumer.New(client, queueUrl).
		WithContext(ctx).
		WithWaitGroup(wg).
		WithBatchSize(10).
		WithReceiveWaitTimeSeconds(5).
		WithReceiveVisibilityTimeout(30).
		WithTerminateVisibilityTimeout(5).
		WithInterval(100).
		WithEnableDebug(true)

	worker := &JobWorker{}

	wg.Add(1)

	go consumerWorker.Worker(worker)

	exit := make(chan os.Signal, 1)

	signal.Notify(exit, syscall.SIGINT, syscall.SIGTERM)

	sig := <-exit

	log.Println("Worker Received Shutdown Signal", sig)

	cancel()

	consumerWorker.Close()

	wg.Wait() // wait for all goroutines

	log.Println("All workers done, shutting down!")
}

type JobWorker struct {
	Name string
}

func (job *JobWorker) HandleMessage(ctx context.Context, record *sqs.Message) error {
	log.Println("Message received : ", record.MessageId, record.Body)
	time.Sleep(time.Second * 30)
	return nil
}
