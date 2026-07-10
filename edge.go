package dagflow

type EdgeFunc func(message map[string]any) (map[string]any, bool)

type Edge struct {
	from string
	to   string
	f    EdgeFunc
}

func (e *Edge) From() string {
	return e.from
}

func (e *Edge) To() string {
	return e.to
}

func (e *Edge) Func() EdgeFunc {
	return e.f
}

func (e *Edge) Do(message map[string]any) (map[string]any, bool) {
	if e.f != nil {
		return e.f(message)
	}
	return nil, false
}
