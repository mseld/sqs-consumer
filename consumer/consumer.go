package consumer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/service/sqs"
)

type Consumer struct {
	paused   bool
	closed   bool
	poolSize int
	interval int
	debug    bool
	ctx      context.Context
	sqs ISqsClient
}

// Handler interface
type Handler interface {
	HandleMessage(ctx context.Context, msg *sqs.Message) error
}

// BatchHandlerFunc batch handler function
type BatchHandlerFunc func(ctx context.Context, message []*sqs.Message) error

// HandlerFunc handler function
type HandlerFunc func(ctx context.Context, message *sqs.Message) error

// HandleMessage is used for the actual execution of each message
func (f HandlerFunc) HandleMessage(ctx context.Context, msg *sqs.Message) error {
	return f(ctx, msg)
}

// New create a new consumer
func New(sqs *sqs.SQS, queueUrl string) IConsumer {
	consumer := &Consumer{
		closed:   false,
		paused:   false,
		debug:    false,
		poolSize: 0,
		interval: 100,
		ctx:      context.Background(),
		sqs:      NewSQSClient(sqs, queueUrl),
	}
	return consumer
}

func (consumer *Consumer) WithContext(ctx context.Context) *Consumer {
	consumer.ctx = ctx
	return consumer
}

func (consumer *Consumer) WithInterval(ms int) *Consumer {
	consumer.interval = ms
	return consumer
}

func (consumer *Consumer) WithEnableDebug(enabled bool) *Consumer {
	consumer.debug = enabled
	return consumer
}

func (consumer *Consumer) WithBatchSize(batchSize int64) *Consumer {
	if consumer.sqs != nil {
		consumer.sqs.WithBatchSize(batchSize)
	}
	return consumer
}

func (consumer *Consumer) WithWaitTimeSeconds(waitTimeSeconds int64) *Consumer {
	if consumer.sqs != nil {
		consumer.sqs.WithWaitTimeSeconds(waitTimeSeconds)
	}
	return consumer
}

func (consumer *Consumer) WithVisibilityTimeout(visibilityTimeout int64) *Consumer {
	if consumer.sqs != nil {
		consumer.sqs.WithVisibilityTimeout(visibilityTimeout)
	}
	return consumer
}

func (consumer *Consumer) Context() context.Context {
	return consumer.ctx
}

func (consumer *Consumer) Resume() {
	consumer.paused = false
}

// Stop processing
func (consumer *Consumer) Pause() {
	consumer.paused = true
}

// Paused check worker is paused
func (consumer *Consumer) Paused() bool {
	return consumer.paused
}

// Close allowing the process to exit gracefully
func (consumer *Consumer) Close() {
	consumer.closed = true
}

// Closed check worker is closed
func (consumer *Consumer) Closed() bool {
	return consumer.closed
}

// Running check if the mq client is running
func (consumer *Consumer) Running() bool {
	return !consumer.Paused() && !consumer.Closed()
}

// WorkerPool worker pool
func (consumer *Consumer) WorkerPool(h Handler, poolSize int) {
	if poolSize <= 0 {
		poolSize = 1
	}

	consumer.poolSize = poolSize

	for w := 1; w <= poolSize; w++ {
		go consumer.worker(h)
	}
}

// Start polling and will continue polling till the application is forcibly stopped
func (consumer *Consumer) Worker(h Handler) {
	// ctx, cancel := context.WithCancel(consumer.ctx)
	go consumer.worker(h)
}

func (consumer *Consumer) worker(h Handler) {
	for {
		select {
		case <-consumer.ctx.Done():
			return
		default:
			if !consumer.Running() {
				time.Sleep(time.Millisecond * time.Duration(consumer.interval))
				continue
			}

			messages, err := consumer.sqs.ReceiveMessageWithContext(consumer.ctx)

			if err != nil {
				consumer.print(ERROR, "Receive Message", err)
				time.Sleep(time.Millisecond * time.Duration(consumer.interval))
				continue
			}

			if len(messages) == 0 {
				consumer.print(INFO, "Receive Message", errors.New("Queue is Empty"))
				time.Sleep(time.Millisecond * time.Duration(consumer.interval))
				continue
			}

			consumer.run(h, messages)
		}
	}
}

// run launches goroutine per received message and wait for all message to be processed
func (consumer *Consumer) run(h Handler, messages []*sqs.Message) {
	var wg *sync.WaitGroup
	numMessages := len(messages)
	wg.Add(numMessages)
	for _, message := range messages {
		go func(m *sqs.Message) {
			defer wg.Done()
			if err := consumer.handleMessage(m, h); err != nil {
				consumer.print(ERROR, "Handle Message", err)
			}
		}(message)
	}

	wg.Wait()
}

func (consumer *Consumer) handleMessage(m *sqs.Message, h Handler) error {
	if err := h.HandleMessage(consumer.ctx, m); err != nil {
		return consumer.sqs.TerminateVisibilityTimeout(m)
	}
	return consumer.sqs.DeleteMessage(m)
}

func (consumer *Consumer) print(level string, message string, err error) {
	if consumer.debug {
		fmt.Println(time.Now().Format(time.RFC3339), level, message, "-", err.Error())
	}
}
