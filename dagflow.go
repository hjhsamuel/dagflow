package dagflow

import "errors"

type Dag struct {
	nodes map[string]NodeItf
	edges map[string]map[string]EdgeFunc
}

// Check
//
// Verify the validity of the DAG.
func (d *Dag) Check() bool {
	if len(d.nodes) == 0 {
		return true
	}

	inDegree := make(map[string]int)
	adj := make(map[string][]string)

	for id := range d.nodes {
		inDegree[id] = 0
	}

	for pre, v := range d.edges {
		for next := range v {
			adj[pre] = append(adj[pre], next)
			inDegree[next] += 1
		}
	}

	var queue []string
	for id, degree := range inDegree {
		if degree == 0 {
			queue = append(queue, id)
		}
	}

	count := 0
	for len(queue) > 0 {
		u := queue[0]
		queue = queue[1:]
		count++

		for _, v := range adj[u] {
			inDegree[v]--
			if inDegree[v] == 0 {
				queue = append(queue, v)
			}
		}
	}

	return count == len(d.nodes)
}

// AddEdge
//
// Add an edge to the DAG.
func (d *Dag) AddEdge(from, to NodeItf, f EdgeFunc) bool {
	if from == nil || to == nil {
		return false
	}

	if v, fok := d.edges[from.ID()]; fok {
		if _, tok := v[to.ID()]; tok {
			return false
		}
	}

	if _, ok := d.nodes[from.ID()]; !ok {
		d.nodes[from.ID()] = from
	}
	if _, ok := d.nodes[to.ID()]; !ok {
		d.nodes[to.ID()] = to
	}

	if _, ok := d.edges[from.ID()]; !ok {
		d.edges[from.ID()] = make(map[string]EdgeFunc)
	}
	if _, ok := d.edges[from.ID()][to.ID()]; !ok {
		d.edges[from.ID()][to.ID()] = f
	}

	return true
}

// New
//
// Copy the DAG and return a new JobItf.
func (d *Dag) New(f NewJob) (JobItf, error) {
	if !d.Check() {
		return nil, errors.New("invalid DAG")
	}
	nodes := make([]NodeItf, 0, len(d.nodes))
	for _, node := range d.nodes {
		nodes = append(nodes, node)
	}
	edges := make([]*Edge, 0, len(d.edges))
	for pre, v := range d.edges {
		for next, ef := range v {
			edges = append(edges, &Edge{
				from: pre,
				to:   next,
				f:    ef,
			})
		}
	}

	if f != nil {
		return f(nodes, edges)
	}

	return NewDefaultJob(nodes, edges)
}

func NewDag() *Dag {
	return &Dag{
		nodes: make(map[string]NodeItf),
		edges: make(map[string]map[string]EdgeFunc),
	}
}
