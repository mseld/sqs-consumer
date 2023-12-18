package consumer

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

var _ Consumerer = (*Consumer)(nil)

type Consumerer interface {
	Resume()
	Pause()
	Close()
	Paused() bool
	Closed() bool
	Running() bool
	Context() context.Context
	WaitGroup() *sync.WaitGroup
	Worker(h Handler)
	WorkerPool(h Handler, poolSize int)
}

type Consumer struct {
	ctx      context.Context
	wg       *sync.WaitGroup
	sqs      Queuer
	paused   bool
	closed   bool
	interval int
	debug    bool
}

// Handler interface
type Handler interface {
	HandleMessage(ctx context.Context, message types.Message) error
}

// BatchHandlerFunc batch handler function
type BatchHandlerFunc func(ctx context.Context, message []types.Message) error

// HandlerFunc handler function
type HandlerFunc func(ctx context.Context, message types.Message) error

// HandleMessage is used for the actual execution of each message
func (f HandlerFunc) HandleMessage(ctx context.Context, message types.Message) error {
	return f(ctx, message)
}

// New create a new consumer
func New(sqs *sqs.Client, queueUrl string, opts ...Option) *Consumer {
	sqsClient := NewSQSClient(sqs, queueUrl)

	consumer := &Consumer{
		interval: 100,
		wg:       &sync.WaitGroup{},
		ctx:      context.Background(),
		sqs:      sqsClient,
	}

	for _, opt := range opts {
		opt(consumer)
	}

	return consumer
}

func (c *Consumer) Context() context.Context {
	return c.ctx
}

func (c *Consumer) WaitGroup() *sync.WaitGroup {
	return c.wg
}

func (c *Consumer) Resume() {
	c.paused = false
}

// Stop processing
func (c *Consumer) Pause() {
	c.paused = true
}

// Paused check worker is paused
func (c *Consumer) Paused() bool {
	return c.paused
}

// Close allowing the process to exit gracefully
func (c *Consumer) Close() {
	c.closed = true
}

// Closed check worker is closed
func (c *Consumer) Closed() bool {
	return c.closed
}

// Running check if the mq client is running
func (c *Consumer) Running() bool {
	return !c.Paused() && !c.Closed()
}

// WorkerPool worker pool
func (c *Consumer) WorkerPool(h Handler, poolSize int) {
	if poolSize <= 0 {
		poolSize = 1
	}

	for w := 1; w <= poolSize; w++ {
		c.wg.Add(1)
		go c.Worker(h)
	}
}

// Worker Start polling and will continue polling till the application is forcibly stopped
func (c *Consumer) Worker(h Handler) {
	defer c.wg.Done()
	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		if c.Closed() {
			return
		}

		if c.Paused() {
			time.Sleep(time.Millisecond * time.Duration(c.interval))
			continue
		}

		messages, err := c.sqs.ReceiveMessage(c.ctx)

		if err != nil {
			c.logError("failed to receive message", err)
			time.Sleep(time.Millisecond * time.Duration(c.interval))
			continue
		}

		c.log("received message", "count", len(messages))

		if len(messages) == 0 {
			time.Sleep(time.Millisecond * time.Duration(c.interval))
			continue
		}

		c.run(h, messages)
	}
}

// run launches goroutine per received message and wait for all message to be processed
func (c *Consumer) run(h Handler, messages []types.Message) {
	wg := &sync.WaitGroup{}
	for index, message := range messages {
		select {
		case <-c.ctx.Done():
			c.terminate(messages[index:])
			return
		default:
		}

		if c.Closed() {
			c.terminate(messages[index:])
			return
		}

		wg.Add(1)
		go func(m types.Message) {
			defer wg.Done()
			if err := c.handleMessage(m, h); err != nil {
				c.logError("failed to handle message", err)
			}
		}(message)
	}
	wg.Wait()
}

func (c *Consumer) terminate(messages []types.Message) {
	if err := c.sqs.TerminateVisibilityTimeoutBatch(c.ctx, messages); err != nil {
		c.logError("failed to terminate message visibility timeout", err)
	}
}

func (c *Consumer) handleMessage(message types.Message, h Handler) error {
	if err := h.HandleMessage(c.ctx, message); err != nil {
		return c.sqs.TerminateVisibilityTimeout(c.ctx, message)
	}
	return c.sqs.DeleteMessage(c.ctx, message)
}

func (c *Consumer) log(message string, args ...any) {
	if c.debug {
		slog.Info(message, args...)
	}
}

func (c *Consumer) logError(message string, err error) {
	if c.debug {
		slog.Error(message, err)
	}
}
