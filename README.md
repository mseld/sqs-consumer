# sqs-consumer

sqs-consumer

### Example

```golang
func main() {
	config := aws.NewConfig().
        WithRegion("eu-west-1").
        WithMaxRetries(3)

	awsSession := session.Must(session.NewSession())

	client := sqs.New(awsSession, config)

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

	signal.Notify(exit, syscall.SIG, syscall.SIGTERM)

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

func (job *JobWorker) HandleMessage(ctx context.Context, message types.Message) error {
    // ...
	return nil
}

```
