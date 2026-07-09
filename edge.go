package dagflow

import "encoding/json"

type EdgeFunc func(message json.RawMessage) (json.RawMessage, bool)

type Edge struct {
	from string
	to   string
	f    EdgeFunc
}

func (e *Edge) Do(message json.RawMessage) (json.RawMessage, bool) {
	if e.f != nil {
		return e.f(message)
	}
	return nil, false
}
