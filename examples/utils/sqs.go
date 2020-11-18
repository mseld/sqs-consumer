package utils

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

func NewSqsClient() *sqs.SQS {
	config := aws.NewConfig().
		WithRegion("eu-west-1").
		WithMaxRetries(3)

	sess := session.Must(session.NewSession())

	return sqs.New(sess, config)
}
