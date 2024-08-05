package opensearch

import (
	"bytes"
	"encoding/json"
	"github.com/google/go-cmp/cmp"
	"github.com/shubhang93/cdcingestor/internal/kafka/models"
	"testing"
)

func Test_encodeJSONLines(t *testing.T) {
	data := []*models.EventKV{{
		Key:   "foo/bar",
		Value: json.RawMessage(`{"key":"value"}`),
	}, {
		Key:   "foo/bar1",
		Value: json.RawMessage(`{"key1":"value1"}`),
	}, {
		Key:   "foo/bar2",
		Value: json.RawMessage(`{"key2":"value2"}`),
	}}

	var dst bytes.Buffer
	err := encodeJSONLines(data, &dst)
	if err != nil {
		t.Errorf("encode error:%v", err)
		return
	}

	want := `{"create":{"_index":"cdc","id":"bar"}}
{"key":"value"}
{"create":{"_index":"cdc","id":"bar1"}}
{"key1":"value1"}
{"create":{"_index":"cdc","id":"bar2"}}
{"key2":"value2"}
`

	got := dst.String()

	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("--Want ++Got:\n%s", diff)
	}

}
