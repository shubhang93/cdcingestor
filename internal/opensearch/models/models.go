package models

type OpenSearchUpdate struct {
	Index  string          `json:"_index"`
	Type   string          `json:"_type"`
	Id     string          `json:"_id"`
	Status int             `json:"status"`
	Error  OpenSearchError `json:"error"`
}

type OpenSearchCreate struct {
	Index  string          `json:"_index"`
	Type   string          `json:"_type"`
	Id     string          `json:"_id"`
	Status int             `json:"status"`
	Error  OpenSearchError `json:"error"`
}

type Shards struct {
	Total      int `json:"total"`
	Successful int `json:"successful"`
	Failed     int `json:"failed"`
}

type OpenSearchIndex struct {
	Index       string `json:"_index"`
	Type        string `json:"_type"`
	Id          string `json:"_id"`
	Version     int    `json:"_version"`
	Result      string `json:"result"`
	SeqNo       int    `json:"_seq_no"`
	PrimaryTerm int    `json:"_primary_term"`
	Status      int    `json:"status"`
	Shards      Shards `json:"_shards"`
}

type OpenSearchItem struct {
	Index  OpenSearchIndex   `json:"index,omitempty"`
	Create *OpenSearchCreate `json:"create,omitempty"`
	Update *OpenSearchUpdate `json:"update,omitempty"`
}

type OpenSearchResponse struct {
	Took   int              `json:"took"`
	Errors bool             `json:"errors"`
	Items  []OpenSearchItem `json:"items"`
}

type OpenSearchError struct {
	Type      string `json:"type"`
	Reason    string `json:"reason"`
	Index     string `json:"index"`
	Shard     string `json:"shard"`
	IndexUuid string `json:"index_uuid"`
}

type OpenSearchMeta struct {
	Index string `json:"_index"`
	ID    string `json:"id"`
}
type OpenSearchActionMetadata struct {
	Create OpenSearchMeta `json:"create"`
}
