package firehose_test

import (
	"fmt"
	"testing"

	fh "github.com/erikreppel/go-firehose"
)

var hose *fh.Producer

func init() {
	conf := fh.Config{
		FireHoseName: "testStream",
		Region:       "us-west-2",
	}
	hose = fh.New(conf)
}

func TestProducerCanPut(t *testing.T) {
	hose.Start()
	for i := 0; i < 1500; i++ {
		err := hose.Put([]byte(fmt.Sprintf("Message %d", i)))
		if err != nil {
			t.Error(err)
			t.Fail()
		}
	}
	hose.Stop()

}
