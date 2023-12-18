package utils

import (
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

func NewSqsClient() *sqs.Client {
	return sqs.New(sqs.Options{
		Region: "eu-west-1",
	})
}
