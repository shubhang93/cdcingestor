package main

import (
	"flag"
	"fmt"
	"github.com/shubhang93/cdcingestor/internal/kafka"
	"os"
)

const usage = `Usage:
cdcingestor kafka -file="stream.jsonl -boostrap-server=localhost:9092 -topic=topic"
cdcingestor opensearch -bootstrap-server=localhost:9092 -topic=topic`

func main() {
	args := os.Args[1:]
	if len(args) < 1 {
		_, _ = fmt.Fprintln(os.Stderr, usage)
		os.Exit(1)
	}
	cdcFlags := flag.NewFlagSet("cdc", flag.ExitOnError)
	filename := cdcFlags.String("file", "", "-file=stream.jsonl")
	bootstrapServer := cdcFlags.String("bootstrap-server", "", "-bootstrap-server=localhost:9092")
	topic := cdcFlags.String("topic", "", "-topic=topic")

	cmd := args[0]
	args = args[1:]
	switch cmd {
	case "kafka":
		if err := cdcFlags.Parse(args); err != nil {
			_, _ = fmt.Fprintln(os.Stderr, usage)
			os.Exit(1)
		}
		if *topic == "" || *bootstrapServer == "" || *filename == "" {
			cdcFlags.PrintDefaults()
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
	case "opensearch":

	}

}
