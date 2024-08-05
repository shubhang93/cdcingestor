package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/go-cmp/cmp"
	"github.com/shubhang93/cdcingestor/internal/kafka/models"
	"math/rand/v2"
	"strings"
	"testing"
	"time"
)

func TestIngestor_Run(t *testing.T) {

	type TestCase struct {
		Input     string
		Want      []models.EventKV
		WantCount int
	}

	var cases = map[string]TestCase{
		"json lines contain valid records": {
			Input: `{"after": {"key": "foo1","value":{"data": "bar"}}}
{"after": {"key": "foo2","value":{"data": "baz"}}}
{"after": {"key": "foo3","value":{"data": "bar"}}}
{"after": {"key": "foo4","value":{"data": "foo42"}}}
{"after": {"key": "foo5","value":{"data": "foobar"}}}`,
			Want: []models.EventKV{{
				Key:   "foo1",
				Value: json.RawMessage(`{"data": "bar"}`),
			}, {
				Key:   "foo2",
				Value: json.RawMessage(`{"data": "baz"}`),
			}, {
				Key:   "foo3",
				Value: json.RawMessage(`{"data": "bar"}`),
			}, {
				Key:   "foo4",
				Value: json.RawMessage(`{"data": "foo42"}`),
			}, {
				Key:   "foo5",
				Value: json.RawMessage(`{"data": "foobar"}`),
			}},
			WantCount: 5,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			stream := strings.NewReader(tc.Input)
			n := rand.Uint32()
			topic := fmt.Sprintf("test_topic_%x", n)

			t.Logf("topic name:%s\n", topic)

			ig := Ingestor{
				Source: stream,
				Config: IngestorConfig{
					BootstrapServer: "localhost:9092",
					Topic:           topic,
				},
			}

			ingestCount, err := ig.Ingest()
			if err != nil {
				t.Errorf("error ingesting:%v", err)
				return
			}

			if diff := cmp.Diff(tc.WantCount, ingestCount); diff != "" {
				t.Errorf("--WantCount ++GotCount\n%s", diff)
				return
			}

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			got, err := consume(ctx, "localhost:9092", topic)
			if err != nil {
				t.Errorf("error consuming:%v", err)
				return
			}

			if diff := cmp.Diff(tc.Want, got); diff != "" {
				t.Errorf("--Want ++Got:\n%s", diff)
			}
		})
	}

}

func consume(ctx context.Context, bootstrapServer string, topic string) ([]models.EventKV, error) {

	kc, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServer,
		"group.id":          "test_consumer",
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		return nil, err
	}
	if err := kc.Subscribe(topic, nil); err != nil {
		return nil, err
	}

	var ekvs []models.EventKV

	for {
		select {
		case <-ctx.Done():
			return ekvs, nil
		default:
		}
		e := kc.Poll(100)
		switch ev := e.(type) {
		case *kafka.Message:
			ekv := models.EventKV{
				Key:   string(ev.Key),
				Value: ev.Value,
			}
			ekvs = append(ekvs, ekv)
		case kafka.Error:
			return nil, ev
		default:
		}
	}
}
