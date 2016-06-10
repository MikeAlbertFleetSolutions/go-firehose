package firehose

import (
	"time"

	"github.com/apex/log"
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

	// Logger: the logger used.
	Logger log.Interface

	// Client is a firehose API instance
	Client firehoseiface.FirehoseAPI
}

func (c *Config) defaults() {
	if c.Logger == nil {
		c.Logger = log.Log
	}

	c.Logger = c.Logger.WithFields(log.Fields{
		"package": "kinesis",
	})

	if c.Region == "" {
		c.Logger.Fatal("Region is required")
	}

	if c.FireHoseName == "" {
		c.Logger.Fatal("StreamName required")
	}
	c.Logger = c.Logger.WithFields(log.Fields{
		"stream": c.FireHoseName,
	})

	if c.Client == nil {
		c.Client = firehose.New(session.New(aws.NewConfig(), &aws.Config{Region: &c.Region}))
	}

	if c.BufferSize == 0 {
		c.BufferSize = maxBatchSize
	}

	if c.BufferSize > maxBatchSize {
		c.Logger.Fatal("BufferSize exceeds 500")
	}

	if c.BacklogSize == 0 {
		c.BacklogSize = maxBatchSize
	}

	if c.FlushInterval == 0 {
		c.FlushInterval = time.Second
	}
}
