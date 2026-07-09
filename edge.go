package dagflow

import "encoding/json"

type EdgeCheck func(message json.RawMessage) (json.RawMessage, bool)

type Edge struct {
	from string
	to   string
	f    EdgeCheck
}
