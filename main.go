package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"io"
	"log"
	"os"
)

type EventKV struct {
	Key   json.RawMessage `json:"key"`
	Value json.RawMessage `json:"value"`
}

type CDCEvent struct {
	After EventKV `json:"after"`
}

func main() {
	f, err := os.Open("stream.jsonl")
	if err != nil {
		log.Fatalln("error opening sample file:", err.Error())
	}
	br := bufio.NewReader(f)
	p, err := newProducer()
	if err != nil {
		log.Fatalln("error creating producer:", err.Error())
	}

	eventsCh := p.Events()
	done := make(chan int)
	go func() {
		deliveryCount := 0
		for e := range eventsCh {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					log.Printf("delivery failed:%s\n", ev.TopicPartition.Error)
				} else {
					deliveryCount++
				}
			case kafka.Error:
				fmt.Println("client err:", ev.Error())
			default:

			}
		}
		done <- deliveryCount
	}()

	var count int
	for {
		jsonl, err := br.ReadBytes('\n')
		var event CDCEvent

		if err != nil && err != io.EOF {
			log.Println("read errored with:", err.Error())
			break
		}

		if err == io.EOF {

		}

		count++
		topic := "cdc_events_1"
		if err := json.Unmarshal(jsonl, &event); err != nil {
			log.Printf("unmarshall error for line %d:%s\n", count, err.Error())
		} else {
			err := p.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{
					Topic:     &topic,
					Partition: kafka.PartitionAny,
				},
				Value: event.After.Value,
				Key:   event.After.Key,
			}, nil)
			if err != nil {
				log.Println("error producing message:", err.Error(), count)
			}
		}
	}

	p.Flush(10000)
	p.Close()
	dc := <-done
	log.Printf("read:%d delivered:%d\n", count, dc)
}

func handleEvent(e CDCEvent) {

}

func newProducer() (*kafka.Producer, error) {
	km := kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"linger.ms":         100,
	}

	p, err := kafka.NewProducer(&km)
	if err != nil {
		return nil, err
	}

	return p, nil

}
