package dagflow

import (
	"errors"
	"sync"
)

type Dag struct {
	nodes map[string]NodeItf
	edges map[string]map[string]EdgeFunc
	mu    sync.RWMutex
}

// Check
//
// Verify the validity of the DAG.
func (d *Dag) Check() bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.check()
}

// check verifies the DAG while the caller holds at least a read lock.
func (d *Dag) check() bool {
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

	queue := make([]string, 0, len(d.nodes))
	for id, degree := range inDegree {
		if degree == 0 {
			queue = append(queue, id)
		}
	}
	if len(queue) != 1 {
		return false
	}

	count := 0
	for head := 0; head < len(queue); head++ {
		u := queue[head]
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
	fromID, toID := from.ID(), to.ID()
	if fromID == "" || toID == "" {
		return false
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if v, fok := d.edges[fromID]; fok {
		if _, tok := v[toID]; tok {
			return false
		}
	}

	if _, ok := d.nodes[fromID]; !ok {
		d.nodes[fromID] = from
	}
	if _, ok := d.nodes[toID]; !ok {
		d.nodes[toID] = to
	}

	if _, ok := d.edges[fromID]; !ok {
		d.edges[fromID] = make(map[string]EdgeFunc)
	}
	d.edges[fromID][toID] = f

	return true
}

// New
//
// Copy the DAG and return a new JobItf.
func (d *Dag) New(f NewJob) (JobItf, error) {
	d.mu.RLock()
	if !d.check() {
		d.mu.RUnlock()
		return nil, errors.New("invalid DAG")
	}
	nodes := make([]NodeItf, 0, len(d.nodes))
	for _, node := range d.nodes {
		nodes = append(nodes, node)
	}
	edgeCount := 0
	for _, outgoing := range d.edges {
		edgeCount += len(outgoing)
	}
	edges := make([]*Edge, 0, edgeCount)
	for pre, v := range d.edges {
		for next, ef := range v {
			edges = append(edges, &Edge{
				from: pre,
				to:   next,
				f:    ef,
			})
		}
	}
	d.mu.RUnlock()

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
