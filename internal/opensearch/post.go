package opensearch

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	kafmodels "github.com/shubhang93/cdcingestor/internal/kafka/models"
	"github.com/shubhang93/cdcingestor/internal/opensearch/models"
	"io"
	"net/http"
	"path"
)

type httpDoer interface {
	Do(req *http.Request) (*http.Response, error)
}

func postBulk(client httpDoer, index string, events []*kafmodels.EventKV) error {
	var body bytes.Buffer
	if err := encodeEvents(events, index, &body); err != nil {
		return fmt.Errorf("error encoding json lines:%w", err)
	}

	url := fmt.Sprintf("http://localhost:9200/%s/_bulk", index)
	req, err := http.NewRequest(http.MethodPost, url, &body)

	if err != nil {
		return fmt.Errorf("error making request:%w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("error sending http request:%w", err)
	}

	if resp.StatusCode != http.StatusOK {
		var temp bytes.Buffer
		_, _ = io.Copy(&temp, resp.Body)
		return fmt.Errorf("http request failed with code:%s\n%s", resp.Status, temp.String())
	}
	defer resp.Body.Close()
	jd := json.NewDecoder(resp.Body)

	var osr models.OpenSearchResponse
	err = jd.Decode(&osr)
	if err != nil {
		return fmt.Errorf("error decoding opensearch response:%w", err)
	}
	if osr.Errors {
		return collateErrors(osr.Items)
	}
	return nil
}

func collateErrors(items []models.OpenSearchItem) error {
	var errs []error
	for _, item := range items {
		if item.Create != nil {
			reason := item.Create.Error.Reason
			errs = append(errs, errors.New(reason))
		}
	}
	return errors.Join(errs...)
}

func encodeEvents(data []*kafmodels.EventKV, index string, dest io.Writer) error {
	je := json.NewEncoder(dest)
	for i, event := range data {
		id := path.Base(event.Key)
		meta := models.OpenSearchMeta{
			Index: index,
			ID:    id,
		}
		err := je.Encode(models.OpenSearchActionMetadata{Create: meta})
		if err != nil {
			return fmt.Errorf("json encode error for meta %d:%w", i, err)
		}

		if err := je.Encode(event.Value); err != nil {
			return fmt.Errorf("json encode error for data %d:%w", i, err)
		}
	}
	return nil
}
