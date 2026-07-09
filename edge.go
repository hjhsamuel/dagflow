package dagflow

type EdgeFunc func(message map[string]any) (map[string]any, bool)

type Edge struct {
	from string
	to   string
	f    EdgeFunc
}

func (e *Edge) Do(message map[string]any) (map[string]any, bool) {
	if e.f != nil {
		return e.f(message)
	}
	return nil, false
}
