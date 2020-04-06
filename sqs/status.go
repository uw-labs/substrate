package sqs

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/service/sqs"

	"github.com/uw-labs/substrate"
)

func sqsStatus(sqsClient *sqs.SQS, queueName string, retryPings int, retryDuration time.Duration) func() (*substrate.Status, error) {
	var status *substrate.Status
	var err error

	return func() (*substrate.Status, error) {
		for ping := 0; ping < retryPings; ping++ {
			status, err = checkQueue(sqsClient, queueName)
			if err == nil {
				break
			}
			time.Sleep(retryDuration)
		}

		return status, err
	}
}

func checkQueue(sqsClient *sqs.SQS, queueName string) (*substrate.Status, error) {
	if sqsClient == nil {
		return &substrate.Status{
			Problems: []string{"no sqs connection"},
			Working:  false,
		}, nil
	}

	if queueName == "" {
		return &substrate.Status{
			Problems: []string{"no queue name"},
			Working:  false,
		}, nil
	}

	_, err := sqsClient.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: &queueName,
	})
	if err == nil {
		return &substrate.Status{
			Working: true,
		}, nil
	}

	return &substrate.Status{
		Problems: []string{fmt.Sprintf("sqs not connected - last error: %v", err)},
		Working:  false,
	}, nil
}
