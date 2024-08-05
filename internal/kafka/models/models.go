package models

import (
	"encoding/json"
)

type EventKV struct {
	Key   string          `json:"key"`
	Value json.RawMessage `json:"value"`
}

type CDCEvent struct {
	After EventKV `json:"after"`
}
