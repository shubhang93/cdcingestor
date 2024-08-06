# cdcingestor

Ingest JSON logs from a JSON stream

## instructions to run

### Tests

```shell
docker-compose up -d
make test
```

### build the binary
```shell
make build
```

### run the opensearch ingestor
```shell
bin/cdcingestor ingest-opensearch \ 
-bootstrap-server=localhost:9092 \ 
-topic=<topic_name> \ 
-opensearch-addr=localhost:9002
```

### run the kakfa ingestor
```shell
bin/cdcingestor push-kafka \ 
-bootstrap-server=localhost:9092 \ 
-topic= <topic_name >\ 
-file=stream.jsonl

```
