package kafka

import (
	"context"
	"errors"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/shubhang93/cdcingestor/internal/kafka/models"
	"io"
	"time"
)

type MsgReader interface {
	ReadMessage(timeout time.Duration) (*kafka.Message, error)
	io.Closer
}

func ReadBatch(ctx context.Context, c MsgReader, timeout time.Duration, batch []*models.EventKV) (int, error) {
	var i int
	timeoutAfter := time.After(timeout)
	for i < cap(batch) {
		select {
		case <-ctx.Done():
			return 0, nil
		case <-timeoutAfter:
			return i, nil
		default:
			msg, err := c.ReadMessage(timeout)
			var kafkaErr kafka.Error
			ok := errors.As(err, &kafkaErr)

			if ok && kafkaErr.IsFatal() {
				return 0, fmt.Errorf("read message fatal error:%w", err)
			}

			if msg == nil {
				continue
			}
			batch[i] = kafkaMsgToEventKV(msg)
			i++
		}
	}
	return i, nil
}

func kafkaMsgToEventKV(msg *kafka.Message) *models.EventKV {
	event := models.EventKV{
		Key:   string(msg.Key),
		Value: msg.Value,
	}
	return &event
}
