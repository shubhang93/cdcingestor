package opensearch

import (
	"context"
	"fmt"
	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/shubhang93/cdcingestor/internal/kafka"
	kafmodels "github.com/shubhang93/cdcingestor/internal/kafka/models"
	"log"
	"net/http"
	"sync"
	"time"
)

type KafkaConfig struct {
	BootstrapServer string
	Topic           string
}

type Config struct {
	Address           string
	IngestIndex       string
	IngestConcurrency int
}

type Ingestor struct {
	KafkaConfig      KafkaConfig
	OpenSearchConfig Config
	consumerInitFunc func(config KafkaConfig) (kafka.MsgReader, error)
	clientInitFunc   func() httpDoer
	hc               httpDoer
	kc               kafka.MsgReader
	sendChan         chan []*kafmodels.EventKV
}

func (ing *Ingestor) Run(ctx context.Context) error {
	if err := ing.init(); err != nil {
		return err
	}

	var wg sync.WaitGroup
	errChan := make(chan error)
	for i := 0; i < ing.OpenSearchConfig.IngestConcurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for chunk := range ing.sendChan {
				if err := appendBulk(ing.hc, ing.OpenSearchConfig.IngestIndex, chunk); err != nil {
					errChan <- fmt.Errorf("bulk post error:%s", err.Error())
				}
				log.Printf("ingested %d records into opensearch\n", len(chunk))
			}
		}()
	}
	defer ing.kc.Close()

	done := make(chan struct{}, 1)
	go func() {
		defer close(done)
		for err := range errChan {
			log.Println(err)
		}
	}()

	batch := make([]*ckafka.Message, 100)
	var readErr error
	run := true
	for run {
		select {
		case <-ctx.Done():
			run = false
		default:
		}
		n, err := kafka.ReadBatch(ctx, ing.kc, time.Millisecond*100, batch)
		if err != nil {
			readErr = err
			break
		}
		if n > 0 {
			log.Printf("read %d messages from %s\n", n, ing.KafkaConfig.Topic)
			data := transformMessages(batch[:n])
			ing.sendChan <- data
			clear(batch)
		}
	}

	close(ing.sendChan)
	wg.Wait()
	close(errChan)
	<-done

	return readErr
}

func transformMessages(messages []*ckafka.Message) []*kafmodels.EventKV {
	res := make([]*kafmodels.EventKV, len(messages))
	for i, message := range messages {
		res[i] = &kafmodels.EventKV{
			Key:   string(message.Key),
			Value: message.Value,
		}
	}
	return res
}

func (ing *Ingestor) init() error {

	ing.sendChan = make(chan []*kafmodels.EventKV)

	if ing.consumerInitFunc == nil {
		ing.consumerInitFunc = newConsumer
	}

	if ing.clientInitFunc == nil {
		ing.clientInitFunc = func() httpDoer {
			return &http.Client{}
		}
	}

	kc, err := ing.consumerInitFunc(ing.KafkaConfig)
	if err != nil {
		return fmt.Errorf("consumer init error:%w", err)
	}
	ing.kc = kc

	client := ing.clientInitFunc()
	ing.hc = client

	return nil

}

func newConsumer(config KafkaConfig) (kafka.MsgReader, error) {
	kc, err := ckafka.NewConsumer(&ckafka.ConfigMap{
		"bootstrap.servers": config.BootstrapServer,
		"group.id":          "cdc_consumer",
		"debug":             "broker",
	})
	if err != nil {
		return nil, fmt.Errorf("error creating consumer:%s", err.Error())
	}
	if err := kc.Subscribe(config.Topic, nil); err != nil {
		return nil, fmt.Errorf("subscription error:%w", err)
	}
	return kc, nil
}
