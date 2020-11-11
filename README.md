# sqs-consumer
sqs-consumer


### Example
```go
func main() {
	config := aws.NewConfig().WithRegion("eu-west-1").WithMaxRetries(3)
	awsSession := session.Must(session.NewSession())
	sqsInstance := sqs.New(awsSession, config)

	sqsConsumer := consumer.New(sqsInstance, &consumer.Config{
		Region:            "eu-west-1",
		QueueUrl:          "https://sqs.eu-west-1.amazonaws.com/123/queue",
		BatchSize:         10,
		WaitTimeSeconds:   10,
		VisibilityTimeout: 30,
		PollingWaitTimeMs: 100,
	})

	sqsConsumer.Start(&JobWorker{})

}

type JobWorker struct {
    Name string
}

func (job *JobWorker) HandleMessage(record *sqs.Message) error {
	return nil
}

```

### ROAD MAP
- Listen to context cancellation
- Enable/Disable print debug logs based on configuration flag
- Implement Close to allow the process to exit gracefully
