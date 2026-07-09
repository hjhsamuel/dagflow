package dagflow

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
)

type JobState int

const (
	JobStateReady JobState = iota + 1
	JobStateRunning
	JobStateCancelled
	JobStateFinished
)

type JobItf interface {
	Execute(message json.RawMessage) error
	Cancel()
	State() JobState
	Done() <-chan struct{}
}

type NewJob func(nodes []NodeItf, edges []*Edge) (JobItf, error)

type Job struct {
	nodes  map[string]NodeItf
	edges  []*Edge
	state  JobState
	mu     sync.RWMutex
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
}

func (j *Job) Execute(message json.RawMessage) error {
	j.mu.Lock()
	if j.state != JobStateReady {
		j.mu.Unlock()
		return errors.New("job is not ready")
	}
	j.state = JobStateRunning
	j.mu.Unlock()

	go func() {
		defer func() {
			j.mu.Lock()
			if j.state == JobStateRunning {
				j.state = JobStateFinished
			}
			close(j.done)
			j.mu.Unlock()
		}()

		inDegree, adj := j.buildExecutionPlan()
		nodeResults := make(map[string]json.RawMessage)
		var resMu sync.Mutex
		ready := make(chan string, len(j.nodes))

		j.prepareInitialNodes(message, inDegree, nodeResults, ready, &resMu)

		j.runScheduler(inDegree, adj, nodeResults, ready, &resMu)
	}()

	return nil
}

func (j *Job) buildExecutionPlan() (map[string]int, map[string][]*Edge) {
	inDegree := make(map[string]int)
	adj := make(map[string][]*Edge)

	for id := range j.nodes {
		inDegree[id] = 0
	}

	for _, edge := range j.edges {
		adj[edge.from] = append(adj[edge.from], edge)
		inDegree[edge.to]++
	}
	return inDegree, adj
}

func (j *Job) prepareInitialNodes(message json.RawMessage, inDegree map[string]int, nodeResults map[string]json.RawMessage, ready chan string, resMu *sync.Mutex) {
	for id, degree := range inDegree {
		if degree == 0 {
			ready <- id
			resMu.Lock()
			nodeResults[id] = message
			resMu.Unlock()
		}
	}
}

func (j *Job) runScheduler(inDegree map[string]int, adj map[string][]*Edge, nodeResults map[string]json.RawMessage, ready chan string, resMu *sync.Mutex) {
	var wg sync.WaitGroup
	inDegreeMu := sync.Mutex{}
	processedCount := 0
	processedMu := sync.Mutex{}
	allDone := make(chan struct{})

	go func() {
		for {
			select {
			case <-j.ctx.Done():
				return
			case nodeID, ok := <-ready:
				if !ok {
					return
				}
				wg.Add(1)
				go j.executeNode(nodeID, inDegree, adj, nodeResults, ready, resMu, &inDegreeMu, &wg, func() {
					processedMu.Lock()
					processedCount++
					if processedCount == len(j.nodes) {
						close(allDone)
					}
					processedMu.Unlock()
				})
			case <-allDone:
				return
			}
		}
	}()

	<-allDone
	wg.Wait()
	close(ready)
}

func (j *Job) executeNode(id string, inDegree map[string]int, adj map[string][]*Edge, nodeResults map[string]json.RawMessage, ready chan string, resMu *sync.Mutex, inDegreeMu *sync.Mutex, wg *sync.WaitGroup, onDone func()) {
	defer wg.Done()
	defer onDone()

	node := j.nodes[id]
	resMu.Lock()
	input, hasInput := nodeResults[id]
	resMu.Unlock()

	var result json.RawMessage
	var err error
	if hasInput {
		result, err = node.Execute(input)
		if err != nil {
			j.Cancel()
			return
		}
	}

	inDegreeMu.Lock()
	defer inDegreeMu.Unlock()
	for _, edge := range adj[id] {
		nextID := edge.to

		activated := false
		var nextMsg json.RawMessage
		if hasInput {
			nextMsg, activated = edge.Do(result)
		}

		if activated {
			resMu.Lock()
			nodeResults[nextID] = nextMsg
			resMu.Unlock()
		}

		inDegree[nextID]--
		if inDegree[nextID] == 0 {
			ready <- nextID
		}
	}
}

func (j *Job) Cancel() {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.state == JobStateReady || j.state == JobStateRunning {
		j.state = JobStateCancelled
		j.cancel()
	}
}

func (j *Job) State() JobState {
	j.mu.RLock()
	defer j.mu.RUnlock()
	return j.state
}

func (j *Job) Done() <-chan struct{} {
	return j.done
}

func NewDefaultJob(nodes []NodeItf, edges []*Edge) (JobItf, error) {
	j := &Job{
		nodes: make(map[string]NodeItf),
		edges: edges,
		state: JobStateReady,
		done:  make(chan struct{}),
	}
	for _, n := range nodes {
		j.nodes[n.ID()] = n
	}
	j.ctx, j.cancel = context.WithCancel(context.Background())
	return j, nil
}
