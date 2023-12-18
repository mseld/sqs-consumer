package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/mseld/sqs-consumer/v2/consumer"
	"github.com/mseld/sqs-consumer/v2/examples/utils"
)

func main() {
	log.Println("Worker Started")

	client := utils.NewSqsClient()

	ctx, cancel := context.WithCancel(context.Background())

	wg := &sync.WaitGroup{}

	queueUrl := "https://sqs.eu-west-1.amazonaws.com/0000000000/demo-queue"

	consumerWorker := consumer.New(
		client,
		queueUrl,
		consumer.WithContext(ctx),
		consumer.WithWaitGroup(wg),
		consumer.WithInterval(100),
		consumer.WithEnableDebug(true),
		consumer.WithBatchSize(10),
		consumer.WithReceiveWaitTimeSeconds(5),
		consumer.WithReceiveVisibilityTimeout(30),
		consumer.WithTerminateVisibilityTimeout(5),
	)

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

func handler(ctx context.Context, message types.Message) error {
	log.Println("message received : ", message.MessageId, message.Body)
	time.Sleep(time.Second * 10)
	log.Println("message processed")
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
