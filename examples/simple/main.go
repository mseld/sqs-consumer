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

	queueUrl := "https://sqs.eu-west-1.amazonaws.com/763224933484/SAM-Chunk-Queue"

	consumerWorker := consumer.New(client, queueUrl).
		WithContext(ctx).
		WithWaitGroup(wg).
		WithBatchSize(10).
		WithReceiveWaitTimeSeconds(5).
		WithReceiveVisibilityTimeout(30).
		WithTerminateVisibilityTimeout(5).
		WithInterval(100).
		WithEnableDebug(true)

	consumerWorker.WorkerPool(consumer.HandlerFunc(handler), 4)

	exit := make(chan os.Signal, 1)

	signal.Notify(exit, syscall.SIGINT, syscall.SIGTERM)

	sig := <-exit

	log.Println("Worker Received Shutdown Signal", sig)

	consumerWorker.Close()

	time.AfterFunc(time.Second*5, cancel)

	setTimeout(wg, time.Second*30)

	log.Println("All workers done, shutting down!")
}

func handler(ctx context.Context, record *sqs.Message) error {
	log.Println("Message received : ", *record.MessageId, *record.Body)
	time.Sleep(time.Second * 10)
	log.Println("Message Proccessed")
	return nil
}

func setTimeout(wg *sync.WaitGroup, timeout time.Duration) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		wg.Wait()
	}()
	select {
	case <-done:
		log.Println("Wait group Done")
		return
	case <-time.After(timeout):
		log.Println("timeout")
		return
	}
}
