package opensearch

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/google/go-cmp/cmp"
	"github.com/shubhang93/cdcingestor/internal/kafka/models"
	"math/rand/v2"
	"net/http"
	"testing"
)

func Test_encodeJSONLines(t *testing.T) {

	type TestCase struct {
		Input  []*models.EventKV
		Action string
		Want   string
	}

	cases := map[string]TestCase{
		"action is create": {
			Action: "create",
			Input: []*models.EventKV{{
				Key:   "foo/bar",
				Value: json.RawMessage(`{"key":"value"}`),
			}, {
				Key:   "foo/bar1",
				Value: json.RawMessage(`{"key1":"value1"}`),
			}, {
				Key:   "foo/bar2",
				Value: json.RawMessage(`{"key2":"value2"}`),
			}},
			Want: `{"create":{"_index":"cdc"}}
{"key":"value"}
{"create":{"_index":"cdc"}}
{"key1":"value1"}
{"create":{"_index":"cdc"}}
{"key2":"value2"}
`,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			var dst bytes.Buffer
			err := encodeEvents(tc.Input, "cdc", &dst)
			if err != nil {
				t.Errorf("encode error:%v", err)
				return
			}
			got := dst.String()
			if diff := cmp.Diff(tc.Want, got); diff != "" {
				t.Errorf("--Want ++Got:\n%s", diff)
			}
		})
	}

}

func Test_openSearchBulkPost(t *testing.T) {
	data := []*models.EventKV{{
		Key:   "node/1234",
		Value: json.RawMessage(`{"id":"1234","city":"foo"}`),
	}, {
		Key:   "node/5678",
		Value: json.RawMessage(`{"id":"5678","city":"bar"}`),
	}}

	n := rand.Uint32()
	index := fmt.Sprintf("index_%x", n)

	t.Logf("using index:%s\n", index)

	err := postBulk(http.DefaultClient, index, data)
	if err != nil {
		t.Errorf("error posting to opensearch:%v", err)
		return
	}
}
