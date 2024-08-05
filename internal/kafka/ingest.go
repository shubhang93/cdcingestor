package kafka

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/shubhang93/cdcingestor/internal/kafka/models"
	"io"
	"log"
)

const producerFlushTimeoutMS = 10000

type Ingestor struct {
	Source io.Reader
	Config IngestorConfig
}

type IngestorConfig struct {
	BootstrapServer string
	Topic           string
	Concurrency     int
	ChunkSize       int
}

func (i *Ingestor) Ingest() (int, error) {
	kp, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": i.Config.BootstrapServer,
		"linger.ms":         25,
	})
	if err != nil {
		return 0, fmt.Errorf("error creating kafka producer:%w", err)
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
	br := bufio.NewReader(i.Source)
	batch := make([]models.EventKV, i.Config.ChunkSize)
	for {
		line, err := br.ReadBytes('\n')
		if err != nil && err != io.EOF {
			return count + 1, fmt.Errorf("error reading line %d:%w", count+1, err)
		}

		if err == io.EOF && len(line) < 1 {
			break
		}

		var event models.CDCEvent
		if err := json.Unmarshal(line, &event); err != nil {
			return count + 1, fmt.Errorf("error unmarshaling cdc event on line:%d:%w", count+1, err)
		}

		batch = append(batch, event.After)

		message := kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &i.Config.Topic,
				Partition: kafka.PartitionAny,
			},
			Value:  event.After.Value,
			Key:    []byte(event.After.Key),
			Opaque: event.After.Key,
		}

		if err := kp.Produce(&message, nil); err != nil {
			return count + 1, fmt.Errorf("kafka produce error for line:%d:%w", count+1, err)
		}
		count++
	}

	unflushed := kp.Flush(producerFlushTimeoutMS)
	if unflushed > 0 {
		return count - unflushed, fmt.Errorf("producer failed to flush %d messages", unflushed)
	}

	return count, nil
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
