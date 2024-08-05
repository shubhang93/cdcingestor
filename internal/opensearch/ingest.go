package opensearch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	conflkafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/shubhang93/cdcingestor/internal/kafka"
	kafmodels "github.com/shubhang93/cdcingestor/internal/kafka/models"
	"github.com/shubhang93/cdcingestor/internal/opensearch/models"
	"io"
	"net/http"
	"path"
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
	kafkaConfig KafkaConfig
	config      Config
	hc          *http.Client
	kc          *conflkafka.Consumer
	sendChan    chan []*kafmodels.EventKV
}

func (ing *Ingestor) Run(ctx context.Context) error {
	kc, err := kafka.NewConsumer(ing.kafkaConfig.BootstrapServer, ing.kafkaConfig.Topic)
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	for i := 0; i < ing.config.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range ing.sendChan {

			}
		}()
	}
	defer kc.Close()

	batch := make([]*kafmodels.EventKV, 100)
	var readErr error
	for {
		n, err := kafka.ReadBatch(ctx, kc, time.Millisecond*100, batch)
		if err != nil {
			break
		}
		if n > 0 {
			data := make([]*kafmodels.EventKV, n)
			copy(data, batch[:n])
			ing.sendChan <- data
			clear(batch)
		}
	}

	wg.Wait()
	return readErr
}

func openSearchBulkPost(client *http.Client, body io.Reader) error {
	req, err := http.NewRequest(http.MethodPost, "http://localhost:9200/cdc/_bulk", body)

	if err != nil {
		return fmt.Errorf("error making request:%w", err)
	}

	req.Header.Set("Content-Type", "application/jsonl")

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("error sending http request:%w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("http request failed with code:%d", resp.StatusCode)
	}
	defer resp.Body.Close()
	jd := json.NewDecoder(resp.Body)

	var osr models.OpenSearchResponse
	err = jd.Decode(&osr)
	if err != nil {
		return fmt.Errorf("error decoding opensearch response:%w", err)
	}
	if osr.Errors {
		return errors.New("open search found some errors")
	}
	return nil
}

func encodeJSONLines(data []*kafmodels.EventKV, dest io.Writer) error {
	je := json.NewEncoder(dest)
	for i, event := range data {
		id := path.Base(event.Key)
		meta := models.OpenSearchMeta{
			Index: "cdc",
			ID:    id,
		}
		err := je.Encode(models.OpenSearchActionMetadata{Create: meta})
		if err != nil {
			return fmt.Errorf("json encode error for meta %d:%w", i, err)
		}

		if err := je.Encode(event.Value); err != nil {
			return fmt.Errorf("json encode error for data %d:%w", i, err)
		}

	}
	return nil
}
