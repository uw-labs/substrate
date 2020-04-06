package sqs

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/pkg/errors"

	"github.com/uw-labs/substrate"
)

var (
	_ substrate.AsyncMessageSink = (*asyncMessageSink)(nil)
)

// AsyncMessageSinkConfig is the configuration parameters for an AsyncMessageSink.
type AsyncMessageSinkConfig struct {
	Region   string
	SecretID string
	Secret   string

	QueueName      string
	MessageGroupID *string // only required for FIFO queues

	WaitTimeSeconds string

	// time duration between pings (min 1 second)
	ConnectionPingInterval time.Duration

	// the client will return an error after this many pings have timed out (min 3)
	ConnectionNumPings int
}

func NewAsyncMessageSink(config AsyncMessageSinkConfig) (substrate.AsyncMessageSink, error) {
	messageGroupID := config.MessageGroupID

	if config.ConnectionPingInterval < time.Second {
		config.ConnectionPingInterval = time.Second
	}

	if config.ConnectionNumPings < 3 {
		config.ConnectionNumPings = 3
	}

	sess := session.Must(session.NewSession())
	awsConfig := aws.NewConfig().
		WithRegion(config.Region).
		WithCredentials(credentials.NewStaticCredentials(config.SecretID, config.Secret, ""))
	sqsClient := sqs.New(sess, awsConfig)

	qURLOutput, err := sqsClient.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: &config.QueueName,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "queue URL for queue name: '%s'", config.QueueName)
	}

	return &asyncMessageSink{
		sqsClient:      sqsClient,
		queueURL:       qURLOutput.QueueUrl,
		messageGroupID: messageGroupID,
		statusFunc:     sqsStatus(sqsClient, config.QueueName, config.ConnectionNumPings, config.ConnectionPingInterval),
	}, nil
}

type asyncMessageSink struct {
	sqsClient      *sqs.SQS
	queueURL       *string
	messageGroupID *string
	statusFunc     func() (*substrate.Status, error)
}

func (p *asyncMessageSink) PublishMessages(ctx context.Context, acks chan<- substrate.Message, messages <-chan substrate.Message) (rerr error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sendErr := make(chan error, 1)

	go func() {
	LOOP:
		for {
			select {
			case <-ctx.Done():
				break LOOP
			case msg := <-messages:
				payload := string(msg.Data())

				_, err := p.sqsClient.SendMessageWithContext(ctx, &sqs.SendMessageInput{
					MessageBody:    &payload,
					MessageGroupId: p.messageGroupID,
					QueueUrl:       p.queueURL,
				})
				if err != nil {
					sendErr <- err
					break LOOP
				}
				acks <- msg
			}
		}
	}()

	select {
	case <-ctx.Done():
		return
	case serr := <-sendErr:
		return serr
	}
}

func (p *asyncMessageSink) Close() error {
	return nil
}

func (p *asyncMessageSink) Status() (*substrate.Status, error) {
	return p.statusFunc()
}
