package dagflow

import "encoding/json"

type JobState int

const (
	JobStateReady JobState = iota + 1
	JobStateRunning
	JobStatePaused
	JobStateCancelled
	JobStateFinished
)

type JobItf interface {
	Execute(message json.RawMessage) error
	Pause()
	Cancel()
	State() JobState
	Done() <-chan struct{}
}

type NewJob func(nodes []NodeItf, edges []*Edge) (JobItf, error)

type Job struct {
	// TODO
}

func (j *Job) Execute(message json.RawMessage) error {
	// TODO
	panic("not implemented")
}

func (j *Job) Pause() {
	// TODO
	panic("not implemented")
}

func (j *Job) Cancel() {
	// TODO
	panic("not implemented")
}

func (j *Job) State() JobState {
	// TODO
	panic("not implemented")
}

func (j *Job) Done() <-chan struct{} {
	// TODO
	panic("not implemented")
}
