package dagflow

import (
	"context"
	"errors"
	"fmt"
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

// JobErrorItf is implemented by jobs that expose an asynchronous execution
// error. It is separate from JobItf to preserve compatibility with custom jobs.
type JobErrorItf interface {
	JobItf
	Err() error
}

type NewJob func(nodes []NodeItf, edges []*Edge) (JobItf, error)

type Job struct {
	nodes map[string]NodeItf
	edges []*Edge
	state JobState
	err   error
	mu    sync.RWMutex

	ctx      context.Context
	cancel   context.CancelFunc
	done     chan struct{}
	doneOnce sync.Once

	inDegree    map[string]int
	adj         map[string][]*Edge
	nodeResults map[string]map[string]any
}

type edgeResult struct {
	edge      *Edge
	message   map[string]any
	activated bool
}

type nodeCompletion struct {
	id       string
	outgoing []edgeResult
	err      error
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
	initial := j.prepareInitialNodes(message)

	go func() {
		j.runScheduler(initial)

		j.mu.Lock()
		if j.state == JobStateRunning {
			j.state = JobStateFinished
		}
		j.mu.Unlock()
		j.finish()
	}()

	return nil
}

func (j *Job) buildExecutionPlan() {
	j.inDegree = make(map[string]int, len(j.nodes))
	j.adj = make(map[string][]*Edge)

	for id := range j.nodes {
		j.inDegree[id] = 0
	}

	for _, edge := range j.edges {
		j.adj[edge.from] = append(j.adj[edge.from], edge)
		j.inDegree[edge.to]++
	}
}

func (j *Job) prepareInitialNodes(message map[string]any) []string {
	j.nodeResults = make(map[string]map[string]any)
	initial := make([]string, 0, 1)
	for id, degree := range j.inDegree {
		if degree == 0 {
			initial = append(initial, id)
			j.nodeResults[id] = maps.Clone(message)
		}
	}
	return initial
}

// runScheduler is the sole owner of execution-plan state. Workers only execute
// user functions and report immutable completion records back to this loop.
func (j *Job) runScheduler(initial []string) {
	completed := make(chan nodeCompletion, max(1, len(j.nodes)))
	active := 0

	schedule := func(id string) {
		input, hasInput := j.nodeResults[id]
		active++
		go j.executeNode(id, input, hasInput, completed)
	}

	if j.ctx.Err() == nil {
		for _, id := range initial {
			schedule(id)
		}
	}

	ctxDone := j.ctx.Done()
	for active > 0 {
		select {
		case completion := <-completed:
			active--
			if completion.err != nil {
				j.fail(completion.err)
			}
			if j.ctx.Err() != nil {
				continue
			}

			for _, result := range completion.outgoing {
				nextID := result.edge.to
				if result.activated {
					j.mergeNodeResult(nextID, result.message)
				}

				j.inDegree[nextID]--
				if j.inDegree[nextID] == 0 {
					schedule(nextID)
				}
			}

		case <-ctxDone:
			// Keep collecting already-running workers so Done is only closed
			// after every worker has actually returned.
			ctxDone = nil
		}
	}
}

func (j *Job) mergeNodeResult(id string, message map[string]any) {
	current, exists := j.nodeResults[id]
	if !exists {
		j.nodeResults[id] = maps.Clone(message)
		return
	}
	if len(message) == 0 {
		return
	}
	if current == nil {
		current = make(map[string]any, len(message))
		j.nodeResults[id] = current
	}
	maps.Copy(current, message)
}

func (j *Job) executeNode(id string, input map[string]any, hasInput bool, completed chan<- nodeCompletion) {
	completion := nodeCompletion{id: id}
	defer func() {
		if recovered := recover(); recovered != nil {
			completion.err = fmt.Errorf("dagflow: node %q panicked: %v", id, recovered)
		}
		completed <- completion
	}()

	select {
	case <-j.ctx.Done():
		return
	default:
	}

	var result map[string]any
	if hasInput {
		var err error
		result, err = j.nodes[id].Execute(j.ctx, input)
		if err != nil {
			completion.err = fmt.Errorf("dagflow: node %q: %w", id, err)
			return
		}
	}

	completion.outgoing = make([]edgeResult, 0, len(j.adj[id]))
	for _, edge := range j.adj[id] {
		var nextMessage map[string]any
		activated := false
		if hasInput {
			nextMessage, activated = edge.Do(result)
		}
		completion.outgoing = append(completion.outgoing, edgeResult{
			edge:      edge,
			message:   nextMessage,
			activated: activated,
		})
	}
}

func (j *Job) fail(err error) {
	j.mu.Lock()
	if j.err == nil {
		j.err = err
	}
	if j.state == JobStateRunning {
		j.state = JobStateCancelled
	}
	j.mu.Unlock()
	j.cancel()
}

func (j *Job) Cancel() {
	j.mu.Lock()
	state := j.state
	if state == JobStateReady || state == JobStateRunning {
		j.state = JobStateCancelled
		j.cancel()
	}
	j.mu.Unlock()

	if state == JobStateReady {
		j.finish()
	}
}

func (j *Job) State() JobState {
	j.mu.RLock()
	defer j.mu.RUnlock()
	return j.state
}

func (j *Job) Err() error {
	j.mu.RLock()
	defer j.mu.RUnlock()
	return j.err
}

// JobError returns a job's asynchronous execution error when it exposes one.
// It returns nil for jobs that do not implement JobErrorItf.
func JobError(job JobItf) error {
	if withError, ok := job.(JobErrorItf); ok {
		return withError.Err()
	}
	return nil
}

func (j *Job) Done() <-chan struct{} {
	return j.done
}

func (j *Job) finish() {
	j.doneOnce.Do(func() { close(j.done) })
}

func NewDefaultJob(nodes []NodeItf, edges []*Edge) (JobItf, error) {
	j := &Job{
		nodes: make(map[string]NodeItf, len(nodes)),
		edges: edges,
		state: JobStateReady,
		done:  make(chan struct{}),
	}
	for _, n := range nodes {
		if n == nil {
			return nil, errors.New("node ID must not be empty")
		}
		id := n.ID()
		if id == "" {
			return nil, errors.New("node ID must not be empty")
		}
		if _, exists := j.nodes[id]; exists {
			return nil, fmt.Errorf("duplicate node ID %q", id)
		}
		j.nodes[id] = n
	}
	j.ctx, j.cancel = context.WithCancel(context.Background())
	return j, nil
}
