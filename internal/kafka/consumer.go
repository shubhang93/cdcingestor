package kafka

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/shubhang93/cdcingestor/internal/kafka/models"
	"time"
)

func NewConsumer(boostrapServer string, topic string) (*kafka.Consumer, error) {
	kc, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": boostrapServer,
	})
	if err != nil {
		return nil, err
	}
	if err := kc.Subscribe(topic, nil); err != nil {
		return nil, err
	}
	return kc, nil
}

type MsgReader interface {
	ReadMessage(timeout time.Duration) (*kafka.Message, error)
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
			if err != nil {
				return i, fmt.Errorf("error reading message")
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
