package dagflow

import (
	"context"
	"errors"
	"maps"
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
	Execute(message map[string]any) error
	Cancel()
	State() JobState
	Done() <-chan struct{}
}

type NewJob func(nodes []NodeItf, edges []*Edge) (JobItf, error)

type Job struct {
	nodes map[string]NodeItf
	edges []*Edge
	state JobState
	mu    sync.RWMutex

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
	wg     sync.WaitGroup

	inDegree    map[string]int
	inDegreeMu  sync.Mutex
	adj         map[string][]*Edge
	nodeResults map[string]map[string]any
	resMu       sync.Mutex
	ready       chan string

	processedCount int
	processedMu    sync.Mutex
	allDone        chan struct{}
}

func (j *Job) Execute(message map[string]any) error {
	j.mu.Lock()
	if j.state != JobStateReady {
		j.mu.Unlock()
		return errors.New("job is not ready")
	}
	j.state = JobStateRunning
	j.mu.Unlock()

	j.buildExecutionPlan()
	j.prepareInitialNodes(message)

	go func() {
		defer func() {
			j.mu.Lock()
			if j.state == JobStateRunning {
				j.state = JobStateFinished
			}
			close(j.done)
			j.mu.Unlock()
		}()

		j.runScheduler()
	}()

	return nil
}

func (j *Job) buildExecutionPlan() {
	j.inDegree = make(map[string]int)
	j.adj = make(map[string][]*Edge)

	for id := range j.nodes {
		j.inDegree[id] = 0
	}

	for _, edge := range j.edges {
		j.adj[edge.from] = append(j.adj[edge.from], edge)
		j.inDegree[edge.to]++
	}
}

func (j *Job) prepareInitialNodes(message map[string]any) {
	j.nodeResults = make(map[string]map[string]any)
	j.ready = make(chan string, len(j.nodes))

	for id, degree := range j.inDegree {
		if degree == 0 {
			j.ready <- id
			j.resMu.Lock()
			j.nodeResults[id] = maps.Clone(message)
			j.resMu.Unlock()
		}
	}
}

func (j *Job) runScheduler() {
	j.processedCount = 0
	j.allDone = make(chan struct{})

	go func() {
		for {
			select {
			case <-j.ctx.Done():
				return
			case nodeID, ok := <-j.ready:
				if !ok {
					return
				}
				j.wg.Add(1)
				go j.executeNode(nodeID)
			case <-j.allDone:
				return
			}
		}
	}()

	select {
	case <-j.ctx.Done():
	case <-j.allDone:
	}
	j.wg.Wait()
	close(j.ready)
}

func (j *Job) executeNode(id string) {
	defer j.wg.Done()
	defer func() {
		j.processedMu.Lock()
		j.processedCount++
		if j.processedCount == len(j.nodes) {
			select {
			case <-j.allDone:
			default:
				close(j.allDone)
			}
		}
		j.processedMu.Unlock()
	}()

	select {
	case <-j.ctx.Done():
		return
	default:
	}

	node := j.nodes[id]
	j.resMu.Lock()
	input, hasInput := j.nodeResults[id]
	j.resMu.Unlock()

	var result map[string]any
	var err error
	if hasInput {
		result, err = node.Execute(j.ctx, input)
		if err != nil {
			j.Cancel()
			return
		}
	}

	j.inDegreeMu.Lock()
	defer j.inDegreeMu.Unlock()
	for _, edge := range j.adj[id] {
		nextID := edge.to

		activated := false
		var nextMsg map[string]any
		if hasInput {
			nextMsg, activated = edge.Do(result)
		}

		if activated {
			j.resMu.Lock()
			if j.nodeResults[nextID] == nil {
				j.nodeResults[nextID] = nextMsg
			} else {
				maps.Copy(j.nodeResults[nextID], nextMsg)
			}
			j.resMu.Unlock()
		}

		j.inDegree[nextID]--
		if j.inDegree[nextID] == 0 {
			select {
			case j.ready <- nextID:
			case <-j.ctx.Done():
			}
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
