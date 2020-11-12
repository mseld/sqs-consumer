# sqs-consumer

sqs-consumer

### Example

```go
func main() {
	config := aws.NewConfig().
        WithRegion("eu-west-1").
        WithMaxRetries(3)

	awsSession := session.Must(session.NewSession())

	client := sqs.New(awsSession, config)

	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()

	queueUrl := "https://sqs.eu-west-1.amazonaws.com/0000000000000/queue"

	consumerWorker := consumer.New(client, queueUrl).
		WithContext(ctx).
		WithBatchSize(10).
		WithWaitTimeSeconds(3).
		WithVisibilityTimeout(30).
		WithInterval(100).
		WithEnableDebug(true)

    worker := JobWorker{}

	consumerWorker.Worker(worker)

	runtime.Goexit()
}

type JobWorker struct {
    Name string
}

func (job *JobWorker) HandleMessage(ctx context.Context, record *sqs.Message) error {
    // ...
	return nil
}

```

### ROAD MAP

-   Listen to context cancellation
