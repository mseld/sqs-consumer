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
)

func main() {
	log.Println("Worker Started")

	client := NewSqsClient()

	ctx, cancel := context.WithCancel(context.Background())

	wg := &sync.WaitGroup{}

	queueUrl := "https://sqs.eu-west-1.amazonaws.com/763224933484/SAM-Chunk-Queue"

	consumerWorker := consumer.New(client, queueUrl).
		WithContext(ctx).
		WithBatchSize(10).
		WithReceiveWaitTimeSeconds(5).
		WithReceiveVisibilityTimeout(30).
		WithTerminateVisibilityTimeout(5).
		WithInterval(100).
		WithEnableDebug(true)

	// wg.Add(1)
	// go consumerWorker.Worker(consumer.HandlerFunc(handler), wg)
	consumerWorker.WorkerPool(consumer.HandlerFunc(handler), wg, 4)

	exit := make(chan os.Signal, 1)

	signal.Notify(exit, syscall.SIGINT, syscall.SIGTERM)

	sig := <-exit

	log.Println("Worker Received Shutdown Signal", sig)

	time.AfterFunc(time.Second*10, cancel)

	setTimeout(wg, time.Second*5)

	consumerWorker.Close()

	log.Println("All workers done, shutting down!")
}

func handler(ctx context.Context, record *sqs.Message) error {
	log.Println("Message received : ", record.MessageId, record.Body)
	time.Sleep(time.Second * 30)
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
