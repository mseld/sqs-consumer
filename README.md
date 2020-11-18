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

	consumerWorker.WorkerPool(consumer.HandlerFunc(handler), wg, 4)

	exit := make(chan os.Signal, 1)

	signal.Notify(exit, syscall.SIGINT, syscall.SIGTERM)

	sig := <-exit

	log.Println("Worker Received Shutdown Signal", sig)

	cancel()

	consumerWorker.Close()

	wg.Wait()

	log.Println("All workers done, shutting down!")
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
