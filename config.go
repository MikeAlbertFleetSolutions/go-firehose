package firehose

import (
	"log"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/firehose"
	"github.com/aws/aws-sdk-go/service/firehose/firehoseiface"
	"github.com/jpillora/backoff"
)

const (
	maxBatchSize = 500
)

// Config ..
type Config struct {
	// StreamName: the name of the Kinesis Firehose.
	FireHoseName string

	// Region is the AWS region of the Kinesis Firehose
	Region string

	// FlushInterval: how often to flush the buffer.
	FlushInterval time.Duration

	// BufferSize: the batch request size (cannot be greater than 500).
	BufferSize int

	// BacklogSize
	BacklogSize int

	// Backoff: strategy to use for failures.
	Backoff backoff.Backoff

	// Client is a firehose API instance
	Client firehoseiface.FirehoseAPI
}

func (c *Config) defaults() {
	if c.Region == "" {
		log.Fatalln("Region is required")
	}

	if c.FireHoseName == "" {
		log.Fatalln("StreamName required")
	}

	if c.Client == nil {
		c.Client = firehose.New(session.New(), &aws.Config{Region: &c.Region})
	}

	if c.BufferSize == 0 {
		c.BufferSize = maxBatchSize
	}

	if c.BufferSize > maxBatchSize {
		log.Fatalln("BufferSize exceeds 500")
	}

	if c.BacklogSize == 0 {
		c.BacklogSize = maxBatchSize
	}

	if c.FlushInterval == 0 {
		c.FlushInterval = time.Second
	}
}
