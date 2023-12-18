package consumer

import (
	"context"
	"sync"
)

type Option func(*Consumer)

func WithContext(ctx context.Context) Option {
	return func(c *Consumer) {
		c.ctx = ctx
	}
}

func WithWaitGroup(wg *sync.WaitGroup) Option {
	return func(c *Consumer) {
		c.wg = wg
	}
}

func WithInterval(interval int) Option {
	return func(c *Consumer) {
		c.interval = interval
	}
}

func WithEnableDebug(enabled bool) Option {
	return func(c *Consumer) {
		c.debug = enabled
	}
}

func WithBatchSize(batchSize int32) Option {
	return func(c *Consumer) {
		if c.sqs != nil {
			c.sqs.SetBatchSize(batchSize)
		}
	}
}

func WithReceiveWaitTimeSeconds(waitSeconds int32) Option {
	return func(c *Consumer) {
		if c.sqs != nil {
			c.sqs.SetReceiveWaitTimeSeconds(waitSeconds)
		}
	}
}

func WithReceiveVisibilityTimeout(visibilityTimeout int32) Option {
	return func(c *Consumer) {
		if c.sqs != nil {
			c.sqs.SetReceiveVisibilityTimeout(visibilityTimeout)
		}
	}
}

func WithTerminateVisibilityTimeout(visibilityTimeout int32) Option {
	return func(c *Consumer) {
		if c.sqs != nil {
			c.sqs.SetTerminateVisibilityTimeout(visibilityTimeout)
		}
	}
}
