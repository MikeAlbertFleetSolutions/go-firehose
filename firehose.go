package firehose

import (
	"time"

	"github.com/apex/log"
	"github.com/aws/aws-sdk-go/service/firehose"
)

// Size limits as defined by http://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecords.html.
const (
	maxRecordSize  = 1 << 20 // 1MiB
	maxRequestSize = 5 << 20 // 5MiB
)

// Producer handles batching of records
type Producer struct {
	Config
	records chan *firehose.Record
	done    chan struct{}
}
