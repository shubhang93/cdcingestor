package opensearch

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"time"
)

type MockConsumer struct {
	ReadMessageFunc func(timeout time.Duration) (*kafka.Message, error)
}

func (m *MockConsumer) ReadMessage(timeout time.Duration) (*kafka.Message, error) {
	return m.ReadMessageFunc(timeout)
}

//func TestReadBatch(t *testing.T) {
//	mc := MockConsumer{
//		ReadMessageFunc: func(timeout time.Duration) (*kafka.Message, error) {
//			return &kafka.Message{}, nil
//		},
//	}
//
//	want := 100
//	batch := make([]*kafka.Message, want)
//	n, err := readBatch(&mc, 100*time.Millisecond, batch)
//	if err != nil {
//		t.Errorf("error reading batch:%v", err)
//		return
//	}
//	if n != want {
//		t.Errorf("expected:%d got:%d", want, n)
//	}
//
//}
