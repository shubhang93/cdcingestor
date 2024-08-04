package kafka

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"io"
	"log"
)

const producerFlushTimeoutMS = 10000

type IngestorConfig struct {
	BootstrapServer string
	Topic           string
	Concurrency     int
	ChunkSize       int
}

type EventKV struct {
	Key   json.RawMessage `json:"key"`
	Value json.RawMessage `json:"value"`
}

type CDCEvent struct {
	After EventKV `json:"after"`
}

type Ingestor struct {
	Source io.Reader
	Config IngestorConfig
}

func (i *Ingestor) Run() error {
	kp, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": i.Config.BootstrapServer,
		"linger.ms":         25,
	})
	if err != nil {
		return fmt.Errorf("error creating kafka producer:%w", err)
	}

	deliveryEvents := kp.Events()
	done := make(chan struct{})
	defer func() {
		kp.Close()
		<-done
	}()

	go func() {
		for delEvent := range deliveryEvents {
			err := handleDeliveryEvent(delEvent)
			if err != nil {
				log.Printf("error delivering:%s\n", err.Error())
			}
		}
		close(done)
	}()

	count := 0
	var eof bool
	br := bufio.NewReader(i.Source)
	batch := make([]EventKV, i.Config.ChunkSize)
	for !eof {
		line, err := br.ReadBytes('\n')
		if err != nil && err != io.EOF {
			return fmt.Errorf("error reading line %d:%w", count+1, err)
		}
		eof = err == io.EOF

		var event CDCEvent
		if err := json.Unmarshal(line, &event); err != nil {
			return fmt.Errorf("error unmarshaling cdc event on line:%d:%w", count+1, err)
		}

		batch = append(batch, event.After)

		message := kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &i.Config.Topic,
				Partition: kafka.PartitionAny,
			},
			Value:  event.After.Value,
			Key:    event.After.Key,
			Opaque: event.After.Key,
		}

		if err := kp.Produce(&message, nil); err != nil {
			return fmt.Errorf("kafka produce error for line:%d:%w", count+1, err)
		}
		count++
	}

	unflushed := kp.Flush(producerFlushTimeoutMS)
	if unflushed > 0 {
		return fmt.Errorf("producer failed to flush %d messages", unflushed)
	}

	return nil
}

func handleDeliveryEvent(e kafka.Event) error {
	switch ev := e.(type) {
	case *kafka.Message:
		evErr := ev.TopicPartition.Error
		if evErr != nil {
			return fmt.Errorf("error delivering message with key:%v:%s", ev.Opaque, evErr.Error())
		}
		return nil
	case kafka.Error:
		return fmt.Errorf("kafka client error:%w", ev)
	default:
		return nil
	}
}
