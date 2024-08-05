package kafka

import (
	"context"
	"errors"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/shubhang93/cdcingestor/internal/kafka/models"
	"testing"
	"time"
)

type MockConsumer struct {
	ReadMessageFunc func(timeout time.Duration) (*kafka.Message, error)
}

func (m *MockConsumer) Close() error {
	return nil
}

func (m *MockConsumer) ReadMessage(timeout time.Duration) (*kafka.Message, error) {
	return m.ReadMessageFunc(timeout)
}

func TestReadBatch(t *testing.T) {
	t.Run("returns messages without errors", func(t *testing.T) {
		mc := MockConsumer{
			ReadMessageFunc: func(timeout time.Duration) (*kafka.Message, error) {
				return &kafka.Message{}, nil
			},
		}

		want := 100
		batch := make([]*models.EventKV, want)
		n, err := ReadBatch(context.Background(), &mc, 100*time.Millisecond, batch)
		if err != nil {
			t.Errorf("error reading batch:%v", err)
			return
		}
		if n != want {
			t.Errorf("expected:%d got:%d", want, n)
		}
	})

	t.Run("read message returns a fatal error", func(t *testing.T) {
		wantErr := kafka.NewError(kafka.ErrAllBrokersDown, "all brokers down", true)
		mc := MockConsumer{
			ReadMessageFunc: func(timeout time.Duration) (*kafka.Message, error) {
				return nil, wantErr
			},
		}

		batch := make([]*models.EventKV, 100)
		_, gotErr := ReadBatch(context.Background(), &mc, 100*time.Millisecond, batch)
		if !errors.Is(gotErr, wantErr) {
			t.Error("errors do not match")
		}
	})

}
