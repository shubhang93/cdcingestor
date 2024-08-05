package main

import (
	"flag"
	"fmt"
	"github.com/shubhang93/cdcingestor/internal/kafka"
	"github.com/shubhang93/cdcingestor/internal/opensearch"
	"os"
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

		if err := flags.Parse(args); err != nil {
			if *topic == "" || *bootstrapServer == "" {
				flags.PrintDefaults()
			}
		}
		ing := opensearch.Ingestor{}
	}

}
