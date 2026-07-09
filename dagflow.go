package dagflow

type Dag struct {
	nodes map[string]NodeItf
	edges map[string]*Edge
}

func (d *Dag) Check() bool {
	// TODO
	panic("not implemented")
}

func (d *Dag) AddEdge(from, to NodeItf) bool {
	// TODO
	panic("not implemented")
}

func (d *Dag) New(f NewJob) (JobItf, error) {
	// TODO
	panic("not implemented")
}

func NewDag() *Dag {
	return &Dag{
		nodes: make(map[string]NodeItf),
		edges: make(map[string]*Edge),
	}
}
