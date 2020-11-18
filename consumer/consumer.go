package consumer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/service/sqs"
)

type IConsumer interface {
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
	WithWaitGroup(wg *sync.WaitGroup) *Consumer
	WithContext(ctx context.Context) *Consumer
	WithInterval(interval int) *Consumer
	WithEnableDebug(enabled bool) *Consumer
	WithBatchSize(batchSize int64) *Consumer
	WithReceiveWaitTimeSeconds(waitSeconds int64) *Consumer
	WithReceiveVisibilityTimeout(visibilityTimeout int64) *Consumer
	WithTerminateVisibilityTimeout(visibilityTimeout int64) *Consumer
}

type Consumer struct {
	paused   bool
	closed   bool
	interval int
	debug    bool
	wg       *sync.WaitGroup
	ctx      context.Context
	sqs      ISqsClient
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
		interval: 100,
		wg:       &sync.WaitGroup{},
		ctx:      context.Background(),
		sqs:      NewSQSClient(sqs, queueUrl),
	}
	return consumer
}

func (con *Consumer) WithContext(ctx context.Context) *Consumer {
	con.ctx = ctx
	return con
}

func (con *Consumer) WithWaitGroup(wg *sync.WaitGroup) *Consumer {
	con.wg = wg
	return con
}

func (con *Consumer) WithInterval(ms int) *Consumer {
	con.interval = ms
	return con
}

func (con *Consumer) WithBatchSize(batchSize int64) *Consumer {
	if con.sqs != nil {
		con.sqs.WithBatchSize(batchSize)
	}
	return con
}

func (con *Consumer) WithEnableDebug(enabled bool) *Consumer {
	con.debug = enabled
	return con
}

func (con *Consumer) WithReceiveWaitTimeSeconds(waitSeconds int64) *Consumer {
	if con.sqs != nil {
		con.sqs.WithReceiveWaitTimeSeconds(waitSeconds)
	}
	return con
}

func (con *Consumer) WithReceiveVisibilityTimeout(visibilityTimeout int64) *Consumer {
	if con.sqs != nil {
		con.sqs.WithReceiveVisibilityTimeout(visibilityTimeout)
	}
	return con
}

func (con *Consumer) WithTerminateVisibilityTimeout(visibilityTimeout int64) *Consumer {
	if con.sqs != nil {
		con.sqs.WithTerminateVisibilityTimeout(visibilityTimeout)
	}
	return con
}

func (con *Consumer) Context() context.Context {
	return con.ctx
}

func (con *Consumer) WaitGroup() *sync.WaitGroup {
	return con.wg
}

func (con *Consumer) Resume() {
	con.paused = false
}

// Stop processing
func (con *Consumer) Pause() {
	con.paused = true
}

// Paused check worker is paused
func (con *Consumer) Paused() bool {
	return con.paused
}

// Close allowing the process to exit gracefully
func (con *Consumer) Close() {
	con.closed = true
}

// Closed check worker is closed
func (con *Consumer) Closed() bool {
	return con.closed
}

// Running check if the mq client is running
func (con *Consumer) Running() bool {
	return !con.Paused() && !con.Closed()
}

// WorkerPool worker pool
func (con *Consumer) WorkerPool(h Handler, poolSize int) {
	if poolSize <= 0 {
		poolSize = 1
	}

	for w := 1; w <= poolSize; w++ {
		con.wg.Add(1)
		go con.Worker(h)
	}
}

// Worker Start polling and will continue polling till the application is forcibly stopped
func (con *Consumer) Worker(h Handler) {
	defer con.wg.Done()
	for {
		select {
		case <-con.ctx.Done():
			return
		default:
		}

		if con.Closed() {
			return
		}

		if con.Paused() {
			time.Sleep(time.Millisecond * time.Duration(con.interval))
			continue
		}

		messages, err := con.sqs.ReceiveMessageWithContext(con.ctx)

		if err != nil {
			con.logError(ERROR, "Receive Message", err)
			time.Sleep(time.Millisecond * time.Duration(con.interval))
			continue
		}

		con.log(INFO, fmt.Sprintf("[ %-2v ] Message Received", len(messages)))

		if len(messages) == 0 {
			time.Sleep(time.Millisecond * time.Duration(con.interval))
			continue
		}

		con.run(h, messages)
	}
}

// run launches goroutine per received message and wait for all message to be processed
func (con *Consumer) run(h Handler, messages []*sqs.Message) {
	wg := &sync.WaitGroup{}
	for index, message := range messages {
		select {
		case <-con.ctx.Done():
			con.terminate(messages[index:])
			return
		default:
		}

		if con.Closed() {
			con.terminate(messages[index:])
			return
		}

		wg.Add(1)
		go func(m *sqs.Message) {
			defer wg.Done()
			if err := con.handleMessage(m, h); err != nil {
				con.logError(ERROR, "Handle Message", err)
			}
		}(message)
	}
	wg.Wait()
}

func (con *Consumer) terminate(messages []*sqs.Message) {
	if err := con.sqs.TerminateVisibilityTimeoutBatch(messages); err != nil {
		con.logError(ERROR, "Terminate Message Visibility Timeout ", err)
	}
}

func (con *Consumer) handleMessage(m *sqs.Message, h Handler) error {
	if err := h.HandleMessage(con.ctx, m); err != nil {
		return con.sqs.TerminateVisibilityTimeout(m)
	}
	return con.sqs.DeleteMessage(m)
}

func (con *Consumer) log(level string, message string) {
	if con.debug {
		fmt.Println(time.Now().Format(time.RFC3339), level, message)
	}
}

func (con *Consumer) logError(level string, message string, err error) {
	if con.debug {
		fmt.Println(time.Now().Format(time.RFC3339), level, message, "-", err.Error())
	}
}
