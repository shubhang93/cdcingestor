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
-topic=cdc_events \ 
-opensearch-addr=localhost:9002

```

