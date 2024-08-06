package opensearch

import (
	"context"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	kafkainternal "github.com/shubhang93/cdcingestor/internal/kafka"
	"github.com/shubhang93/cdcingestor/internal/kafka/models"
	"github.com/stretchr/testify/mock"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

type mockHTTPClient struct {
	mock.Mock
}

func (m *mockHTTPClient) Do(req *http.Request) (*http.Response, error) {
	args := m.Called(req)
	return args.Get(0).(*http.Response), args.Error(1)
}

type mockMessageReader struct {
	mock.Mock
}

func (m *mockMessageReader) ReadMessage(timeout time.Duration) (*kafka.Message, error) {
	args := m.Called(timeout)
	if args.Get(0) == nil {
		return nil, nil
	}
	return args.Get(0).(*kafka.Message), args.Error(1)
}

func (m *mockMessageReader) Close() error {
	return nil
}

func TestIngestor_Run(t *testing.T) {
	t.Run("post json records from kafka to opensearch", func(t *testing.T) {
		client := mockHTTPClient{}
		reader := mockMessageReader{}
		ing := Ingestor{
			KafkaConfig: KafkaConfig{
				BootstrapServer: "localhost:9092",
				Topic:           "test_topic",
			},
			OpenSearchConfig: Config{
				Address:           "localhost:9002",
				IngestConcurrency: 1,
			},
			consumerInitFunc: func(config KafkaConfig) (kafkainternal.MsgReader, error) {
				return &reader, nil
			},
			clientInitFunc: func() httpDoer {
				return &client
			},
			sendChan: make(chan []*models.EventKV),
		}

		topic := "foo"
		expectedMessages := []*kafka.Message{{
			TopicPartition: kafka.TopicPartition{
				Topic: &topic,
			},
			Key:   []byte("foo1"),
			Value: []byte(`{"key1":"value1"}`),
		}, {
			TopicPartition: kafka.TopicPartition{
				Topic: &topic,
			},
			Key:   []byte("foo2"),
			Value: []byte(`{"key2":"value2"}`),
		}, {
			TopicPartition: kafka.TopicPartition{
				Topic: &topic,
			},
			Key:   []byte("foo3"),
			Value: []byte(`{"key3":"value3"}`),
		}}
		reader.On("ReadMessage", mock.Anything).Return(expectedMessages[0], nil).Once()
		reader.On("ReadMessage", mock.Anything).Return(expectedMessages[1], nil).Once()
		reader.On("ReadMessage", mock.Anything).Return(expectedMessages[2], nil).Once()
		reader.On("ReadMessage", mock.Anything).Return(nil, nil)

		client.On("Do", mock.AnythingOfType("*http.Request")).Return(
			&http.Response{
				StatusCode: 200,
				Body:       io.NopCloser(strings.NewReader(`{}`)),
			},
			nil)

		ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
		defer cancel()
		if err := ing.Run(ctx); err != nil {
			t.Errorf("run error:%v", err)
		}
	})

	t.Run("should not call the opensearch post API if no records were consumed from kafka", func(t *testing.T) {

		defer func() {
			if r := recover(); r != nil {
				t.Errorf("opensearch API was called")
			}
		}()

		client := mockHTTPClient{}
		reader := mockMessageReader{}
		ing := Ingestor{
			KafkaConfig: KafkaConfig{
				BootstrapServer: "localhost:9092",
				Topic:           "test_topic",
			},
			OpenSearchConfig: Config{
				Address:           "localhost:9002",
				IngestConcurrency: 1,
			},
			consumerInitFunc: func(config KafkaConfig) (kafkainternal.MsgReader, error) {
				return &reader, nil
			},
			clientInitFunc: func() httpDoer {
				return &client
			},
			sendChan: make(chan []*models.EventKV),
		}

		reader.On("ReadMessage", mock.Anything).Return(nil, nil)

		client.On("Do", mock.AnythingOfType("*http.Request")).Return(
			&http.Response{
				StatusCode: 200,
				Body:       io.NopCloser(strings.NewReader(`{}`)),
			},
			nil)

		ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
		defer cancel()
		if err := ing.Run(ctx); err != nil {
			t.Errorf("run error:%v", err)
		}
	})

}
