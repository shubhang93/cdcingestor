package kafka

import (
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"math/rand"
	"reflect"
	"strings"
	"testing"
)

func TestIngestor_Run(t *testing.T) {

	jsonLines := `{"after": {"key": "foo1","value":{"data": "bar"}}}
{"after": {"key": "foo2","value":{"data": "baz"}}}
{"after": {"key": "foo3","value":{"data": "bar"}}}
{"after": {"key": "foo4","value":{"data": "foo42"}}}
{"after": {"key": "foo5","value":{"data": "foobar"}}}`

	stream := strings.NewReader(jsonLines)

	n := rand.Uint32()
	topic := fmt.Sprintf("test_topic_%x", n)

	ig := Ingestor{
		Source: stream,
		Config: IngestorConfig{
			BootstrapServer: "localhost:9092",
			Topic:           topic,
		},
	}
	err := ig.Run()
	if err != nil {
		t.Errorf("error ingesting:%v", err)
		return
	}

	var want []EventKV
	jd := json.NewDecoder(stream)
	for jd.More() {
		var ekv EventKV
		if err := jd.Decode(&ekv); err != nil {
			t.Errorf("error decoding json")
			return
		}
		want = append(want, ekv)
	}

	got, err := consume("localhost:9092", topic)
	if err != nil {
		t.Errorf("error consuming:%v", err)
		return
	}

	if !reflect.DeepEqual(want, got) {
		t.Error("want != got")
	}

}

func consume(bootstrapServer string, topic string) ([]EventKV, error) {
	kc, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServer,
		"group.id":          "test_consumer",
	})
	if err != nil {
		return nil, err
	}
	if err := kc.Subscribe(topic, nil); err != nil {
		return nil, err
	}

	var ekvs []EventKV
	e := kc.Poll(100)
	switch ev := e.(type) {
	case *kafka.Message:
		ekv := EventKV{
			Key:   ev.Key,
			Value: ev.Value,
		}
		ekvs = append(ekvs, ekv)
	case kafka.Error:
		return nil, ev
	default:

	}
	return ekvs, nil
}
