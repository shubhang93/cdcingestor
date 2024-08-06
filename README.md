# cdcingestor

Ingest JSON logs from a JSON stream

## Instructions to run

### Tests

```shell
docker-compose up -d
make test
```

### Build the binary

```shell
make build
```

### Run the Opensearch ingestor

```shell
bin/cdcingestor ingest-opensearch \ 
-bootstrap-server=localhost:9092 \ 
-topic=<topic_name> \ 
-opensearch-addr=localhost:9002
```

### Run the Kakfa ingestor

```shell
bin/cdcingestor push-kafka \ 
-bootstrap-server=localhost:9092 \ 
-topic= <topic_name >\ 
-file=stream.jsonl

```
