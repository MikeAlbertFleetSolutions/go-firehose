package firehose

import (
	"errors"
	"time"

	"github.com/apex/log"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/firehose"
)

// Size limits as defined by http://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecords.html.
const (
	maxRecordSize  = 1 << 20 // 1MiB
	maxRequestSize = 5 << 20 // 5MiB
)

// Errors
var (
	ErrRecordSizeExceeded = errors.New("kinesis: record size exceeded")
)

// New producer with given config
func New(c Config) *Producer {
	c.defaults()
	return &Producer{
		Config:  c,
		records: make(chan *firehose.Record, c.BacklogSize),
		done:    make(chan struct{}),
	}
}

// Producer handles batching of records
type Producer struct {
	Config
	records chan *firehose.Record
	done    chan struct{}
}

// Put record `data` using `partitionKey`. This method is thread-safe.
func (p *Producer) Put(data []byte) error {
	if len(data) > maxRecordSize {
		return ErrRecordSizeExceeded
	}

	p.records <- &firehose.Record{
		Data: data,
	}

	return nil
}

// Start the producer
func (p *Producer) Start() {
	go p.loop()
}

// Stop the producer. Flushes any in-flight data.
func (p *Producer) Stop() {
	p.Logger.WithField("backlog", len(p.records)).Info("stopping producer")

	// drain
	p.done <- struct{}{}
	close(p.records)

	// wait
	<-p.done

	p.Logger.Info("stopped producer")
}

func (p *Producer) loop() {
	buf := make([]*firehose.Record, 0, p.BufferSize)
	tick := time.NewTicker(p.FlushInterval)
	drain := false

	defer func() {
		if len(buf) > 0 {
			p.flush(buf, "final")
		}
	}()
	defer tick.Stop()
	defer close(p.done)

	for {
		select {
		case record := <-p.records:
			buf = append(buf, record)

			if len(buf) >= p.BufferSize {
				p.flush(buf, "buffer size")
				buf = nil
			}

			if drain && len(p.records) == 0 {
				return
			}
		case <-tick.C:
			if len(buf) > 0 {
				p.flush(buf, "interval")
				buf = nil
			}
		case <-p.done:
			drain = true

			if len(p.records) == 0 {
				return
			}
		}
	}
}

// flush records and retry failures if necessary.
func (p *Producer) flush(records []*firehose.Record, reason string) {
	p.Logger.WithFields(log.Fields{
		"package": "firehose",
		"records": len(records),
		"reason":  reason,
	}).Debug("flush")

	params := &firehose.PutRecordBatchInput{
		DeliveryStreamName: aws.String(p.FireHoseName),
		Records:            records,
	}

	out, err := p.Client.PutRecordBatch(params)

	if err != nil {
		p.Logger.WithError(err).Error("flush")
		p.backoff(len(records))
		p.flush(records, "error")
		return
	}

	failed := *out.FailedPutCount
	if failed == 0 {
		p.Backoff.Reset()
		return
	}
	p.backoff(int(failed))
	p.flush(failures(records, out.RequestResponses), "retry")
}

// calculates backoff duration and pauses execution
func (p *Producer) backoff(failed int) {
	backoff := p.Backoff.Duration()

	p.Logger.WithFields(log.Fields{
		"failures": failed,
		"backoff":  backoff,
	}).Warn("put failures")

	time.Sleep(backoff)
}

// failures returns the failed records as indicated in the response.
func failures(records []*firehose.Record, response []*firehose.PutRecordBatchResponseEntry) (
	out []*firehose.Record) {
	for i, record := range response {
		if record.ErrorCode != nil {
			out = append(out, records[i])
		}
	}
	return
}
