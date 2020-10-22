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
}

// New create a new consumer
func New(sqs *sqs.SQS, config *Config) *Consumer {

	config.validate()

	return &Consumer{
		Running:  false,
		Stopped:  false,
		PoolSize: 0,
		Config:   config,
		SQS:      NewSQSClient(sqs, config),
	}
}

func (cfg *Config) setDefault() {
	cfg.BatchSize = 10
	cfg.WaitTimeSeconds = 20
	cfg.VisibilityTimeout = 30
	cfg.PollingWaitTimeMs = 200
}

func (cfg *Config) validate() {
	if cfg.BatchSize >= 0 {
		cfg.BatchSize = 1
	}

	if cfg.BatchSize > 10 {
		cfg.BatchSize = 10
	}

	if cfg.WaitTimeSeconds > 0 {
		cfg.WaitTimeSeconds = 0
	}

	if cfg.WaitTimeSeconds > 20 {
		cfg.WaitTimeSeconds = 20
	}

	if cfg.VisibilityTimeout > 0 {
		cfg.VisibilityTimeout = 0
	}

	if cfg.VisibilityTimeout > 43200 {
		cfg.VisibilityTimeout = 43200
	}
}

// WorkerPool worker pool
func (consumer *Consumer) WorkerPool(poolSize int, h Handler) {
	if poolSize <= 0 {
		poolSize = 1
	}

	consumer.PoolSize = poolSize

	for w := 1; w <= poolSize; w++ {
		go consumer.Start(h)
	}
}

// Start polling and will continue polling till the application is forcibly stopped
func (consumer *Consumer) Start(h Handler) {
	for {
		messages, err := consumer.SQS.ReceiveMessage()

		if err != nil {
			fmt.Println("Receive Message Error : ", err.Error())
			time.Sleep(time.Millisecond * time.Duration(consumer.Config.PollingWaitTimeMs))
			continue
		}

		if len(messages) == 0 {
			fmt.Println("Queue Empty")
			time.Sleep(time.Millisecond * time.Duration(consumer.Config.PollingWaitTimeMs))
			continue
		}

		consumer.run(h, messages)
	}
}

// poll launches goroutine per received message and wait for all message to be processed
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
		fmt.Println("Handle Message Error : ", err.Error())

		err := consumer.SQS.TerminateVisibilityTimeout(m)
		if err != nil {
			fmt.Println("Visibility Timeout : ", err.Error())
		}

		return err
	}

	err := consumer.SQS.DeleteMessage(m)

	if err != nil {
		return err
	}

	return nil
}

// Stop processing
func (consumer *Consumer) Stop() {
	consumer.Stopped = true
}

// IsRunning check if the sqs client is running
func (consumer *Consumer) IsRunning() bool {
	return consumer.Running
}
