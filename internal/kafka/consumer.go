package kafka

import (
	"context"
	"errors"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"io"
	"time"
)

type MsgReader interface {
	ReadMessage(timeout time.Duration) (*kafka.Message, error)
	io.Closer
}

func ReadBatch(ctx context.Context, c MsgReader, timeout time.Duration, batch []*kafka.Message) (int, error) {
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
			batch[i] = msg
			i++
		}
	}
	return i, nil
}
