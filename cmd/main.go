package main

import (
	"cmp"
	"context"
	"flag"
	"fmt"
	"github.com/shubhang93/cdcingestor/internal/kafka"
	"github.com/shubhang93/cdcingestor/internal/opensearch"
	"os"
	"os/signal"
	"syscall"
)

const usage = `Usage:
cdcingestor push-kafka -file="stream.jsonl -boostrap-server=localhost:9092 -topic=topic"
cdcingestor ingest-opensearch -bootstrap-server=localhost:9092 -topic=topic`

func main() {
	args := os.Args[1:]
	if len(args) < 1 {
		_, _ = fmt.Fprintln(os.Stderr, usage)
		os.Exit(1)
	}

	cmd := args[0]
	args = args[1:]
	switch cmd {
	case "push-kafka":
		flags := flag.NewFlagSet("kafka", flag.ExitOnError)
		bootstrapServer := flags.String("bootstrap-server", "", "-bootstrap-server=localhost:9092")
		topic := flags.String("topic", "", "-topic=topic")
		filename := flags.String("file", "", "-file=stream.jsonl")

		if err := flags.Parse(args); err != nil {
			_, _ = fmt.Fprintln(os.Stderr, usage)
			os.Exit(1)
		}
		if *topic == "" || *bootstrapServer == "" || *filename == "" {
			flags.PrintDefaults()
			os.Exit(1)
		}

		file, err := os.Open(*filename)
		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err.Error())
			os.Exit(1)
		}

		kig := kafka.Ingestor{
			Source: file,
			Config: kafka.IngestorConfig{
				BootstrapServer: *bootstrapServer,
				Topic:           *topic,
			},
		}
		n, err := kig.Ingest()
		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err.Error())
			os.Exit(1)
		}
		_, _ = fmt.Fprintf(os.Stdout, "ingested %d records", n)
	case "ingest-opensearch":
		flags := flag.NewFlagSet("kafka", flag.ExitOnError)
		bootstrapServer := flags.String("bootstrap-server", "", "-bootstrap-server=localhost:9092")
		topic := flags.String("topic", "", "-topic=topic")
		openSearchAddr := flags.String("opensearch-addr", "", "-opensearch-addr=localhost:9002")
		ingestIndex := flags.String("ingest-index", "", "-ingest-index=index")

		if err := flags.Parse(args); err != nil {
			if *topic == "" || *bootstrapServer == "" || *openSearchAddr == "" {
				flags.PrintDefaults()
				os.Exit(1)
			}
		}
		ing := opensearch.Ingestor{
			KafkaConfig: opensearch.KafkaConfig{
				BootstrapServer: *bootstrapServer,
				Topic:           *topic,
			},
			OpenSearchConfig: opensearch.Config{
				Address:           *openSearchAddr,
				IngestIndex:       cmp.Or(*ingestIndex, "cdc"),
				IngestConcurrency: 2,
			},
		}

		ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
		defer cancel()
		err := ing.Run(ctx)
		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err.Error())
			os.Exit(1)
		}
	default:
		_, _ = fmt.Fprintf(os.Stderr, "unknown command:%s\n", cmd)
		os.Exit(1)
	}

}
