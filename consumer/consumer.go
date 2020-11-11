package consumer

import (
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/service/sqs"
)

// BatchHandlerFunc batch handler function
type BatchHandlerFunc func(message []*sqs.Message) error

// HandlerFunc handler function
type HandlerFunc func(message *sqs.Message) error

// HandleMessage is used for the actual execution of each message
func (f HandlerFunc) HandleMessage(msg *sqs.Message) error {
	return f(msg)
}

// Handler interface
type Handler interface {
	HandleMessage(msg *sqs.Message) error
	// HandleMessageWithContext(ctx context.Context, msg *sqs.Message) error
}

// New create a new consumer
func New(sqs *sqs.SQS, config *Config) IConsumer {

	config.validate()

	consumer := &Consumer{
		closed:   false,
		paused:   true,
		poolSize: 0,
		config:   config,
		mq:    NewSQSClient(sqs, config),
	}

	return consumer
}

// WorkerPool worker pool
func (consumer *Consumer) WorkerPool(poolSize int, h Handler) {
	if poolSize <= 0 {
		poolSize = 1
	}

	consumer.poolSize = poolSize

	for w := 1; w <= poolSize; w++ {
		go consumer.Worker(h)
	}
}

// Start polling and will continue polling till the application is forcibly stopped
func (consumer *Consumer) Worker(h Handler) {
	for {
		if consumer.Running() {
			messages, err := consumer.mq.ReceiveMessage()

			if err != nil {
				fmt.Println("Receive Message Error : ", err.Error())
				time.Sleep(time.Millisecond * time.Duration(consumer.config.PollingWaitTimeMs))
				continue
			}

			if len(messages) == 0 {
				fmt.Println("Queue Empty")
				time.Sleep(time.Millisecond * time.Duration(consumer.config.PollingWaitTimeMs))
				continue
			}

			consumer.run(h, messages)
		}
		time.Sleep(time.Millisecond * time.Duration(consumer.config.PollingWaitTimeMs))
	}
}

// run launches goroutine per received message and wait for all message to be processed
func (consumer *Consumer) run(h Handler, messages []*sqs.Message) {
	var wg sync.WaitGroup
	numMessages := len(messages)
	wg.Add(numMessages)
	for _, message := range messages {
		go func(m *sqs.Message) {
			defer wg.Done()
			if err := consumer.handleMessage(m, h); err != nil {
				fmt.Println("Handle Message Error : ", err.Error())
			}
		}(message)
	}

	wg.Wait()
}

func (consumer *Consumer) handleMessage(m *sqs.Message, h Handler) error {
	if err := h.HandleMessage(m); err != nil {
		return consumer.mq.TerminateVisibilityTimeout(m)
	}
	return consumer.mq.DeleteMessage(m)
}

func (consumer *Consumer) Start() {
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
