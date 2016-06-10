package firehose_test

import (
	"fmt"
	"testing"
	"time"

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

func TestProducerTicks(t *testing.T) {
	hose.Start()
	for i := 0; i < 3; i++ {
		time.Sleep(time.Second * 3)
		for i := 0; i < 300; i++ {
			err := hose.Put([]byte(fmt.Sprintf("Message %d", i)))
			if err != nil {
				t.Error(err)
				t.Fail()
			}
		}
	}
	hose.Stop()
}
