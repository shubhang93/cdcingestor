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
	Address     string
	Concurrency int
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
	var wg sync.WaitGroup
	for i := 0; i < ing.OpenSearchConfig.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for chunk := range ing.sendChan {
				if err := postBulk(ing.hc, "cdc", chunk); err != nil {
					log.Println("bulk post failed with error:", err.Error())
				}
			}
		}()
	}
	defer ing.kc.Close()

	batch := make([]*kafmodels.EventKV, 100)
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
			data := make([]*kafmodels.EventKV, n)
			copy(data, batch[:n])
			ing.sendChan <- data
			clear(batch)
		}
	}
	close(ing.sendChan)
	wg.Wait()
	return readErr
}

func (ing *Ingestor) init() error {

	if ing.consumerInitFunc == nil {
		ing.consumerInitFunc = func(config KafkaConfig) (kafka.MsgReader, error) {
			return ckafka.NewConsumer(&ckafka.ConfigMap{
				"boostrap.servers": config.BootstrapServer,
				"group.id":         "cdc_consumer",
			})
		}
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
