package dagflow

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestDag_Check(t *testing.T) {
	t.Run("Valid DAG with one start node", func(t *testing.T) {
		d := NewDag()
		n1 := NewNode("n1", nil)
		n2 := NewNode("n2", nil)
		n3 := NewNode("n3", nil)

		d.AddEdge(n1, n2, nil)
		d.AddEdge(n2, n3, nil)

		if !d.Check() {
			t.Error("Expected DAG to be valid")
		}
	})

	t.Run("Invalid DAG with cycle", func(t *testing.T) {
		d := NewDag()
		n1 := NewNode("n1", nil)
		n2 := NewNode("n2", nil)
		n3 := NewNode("n3", nil)

		d.AddEdge(n1, n2, nil)
		d.AddEdge(n2, n3, nil)
		d.AddEdge(n3, n1, nil)

		if d.Check() {
			t.Error("Expected DAG to be invalid (cycle)")
		}
	})

	t.Run("Invalid DAG with multiple start nodes", func(t *testing.T) {
		d := NewDag()
		n1 := NewNode("n1", nil)
		n2 := NewNode("n2", nil)
		n3 := NewNode("n3", nil)
		n4 := NewNode("n4", nil)

		d.AddEdge(n1, n2, nil)
		d.AddEdge(n3, n4, nil)

		if d.Check() {
			t.Error("Expected DAG to be invalid (multiple start nodes)")
		}
	})

	t.Run("Invalid DAG with no start nodes", func(t *testing.T) {
		d := NewDag()
		n1 := NewNode("n1", nil)
		n2 := NewNode("n2", nil)

		// Self loop or cycle without entry
		d.AddEdge(n1, n2, nil)
		d.AddEdge(n2, n1, nil)

		if d.Check() {
			t.Error("Expected DAG to be invalid (no start nodes)")
		}
	})
}

func TestJob_ExecuteAsync(t *testing.T) {
	d := NewDag()
	count := 0
	mu := sync.Mutex{}
	f := func(ctx context.Context, message map[string]any) (map[string]any, error) {
		mu.Lock()
		count++
		mu.Unlock()
		return message, nil
	}

	n1 := NewNode("n1", f)
	n2 := NewNode("n2", f)
	n3 := NewNode("n3", f)

	// n1 -> n2 (always true)
	// n1 -> n3 (always false)
	d.AddEdge(n1, n2, func(message map[string]any) (map[string]any, bool) {
		return message, true
	})
	d.AddEdge(n1, n3, func(message map[string]any) (map[string]any, bool) {
		return nil, false
	})

	job, err := d.New(NewDefaultJob)
	if err != nil {
		t.Fatalf("Failed to create job: %v", err)
	}

	err = job.Execute(map[string]any{"data": "hello"})
	if err != nil {
		t.Fatalf("Job execution failed: %v", err)
	}

	// Wait for done
	select {
	case <-job.Done():
	case <-time.After(time.Second):
		t.Fatal("Timeout waiting for job to finish")
	}

	if job.State() != JobStateFinished {
		t.Errorf("Expected job state to be Finished, got %v", job.State())
	}

	// n1 executes, n2 executes (edge true), n3 skips (edge false)
	// Total count should be 2
	if count != 2 {
		t.Errorf("Expected 2 nodes to be executed, got %d", count)
	}
}

func TestJob_ParallelExecution(t *testing.T) {
	d := NewDag()
	start := time.Now()
	f := func(ctx context.Context, message map[string]any) (map[string]any, error) {
		time.Sleep(100 * time.Millisecond)
		return nil, nil
	}

	n1 := NewNode("n1", f)
	n2 := NewNode("n2", f)
	n3 := NewNode("n3", f)

	// n1 -> n2
	// n1 -> n3
	// n2 and n3 should run in parallel
	d.AddEdge(n1, n2, func(message map[string]any) (map[string]any, bool) { return nil, true })
	d.AddEdge(n1, n3, func(message map[string]any) (map[string]any, bool) { return nil, true })

	job, _ := d.New(NewDefaultJob)
	job.Execute(nil)
	<-job.Done()

	duration := time.Since(start)
	// Expected: n1 (100ms) + max(n2, n3) (100ms) = ~200ms
	// If serial: 300ms
	if duration > 250*time.Millisecond {
		t.Errorf("Execution took too long: %v, expected parallel execution", duration)
	}
}

func TestJob_DataPassing(t *testing.T) {
	d := NewDag()
	var finalMsg string
	mu := sync.Mutex{}

	n1 := NewNode("n1", func(ctx context.Context, message map[string]any) (map[string]any, error) {
		return map[string]any{"data": "data from n1"}, nil
	})
	n2 := NewNode("n2", func(ctx context.Context, message map[string]any) (map[string]any, error) {
		mu.Lock()
		finalMsg = fmt.Sprintf("%v", message["data"])
		mu.Unlock()
		return nil, nil
	})

	d.AddEdge(n1, n2, func(message map[string]any) (map[string]any, bool) {
		// Transform message in edge
		msg := "transformed " + fmt.Sprintf("%v", message["data"])
		return map[string]any{"data": msg}, true
	})

	job, _ := d.New(NewDefaultJob)
	job.Execute(nil)
	<-job.Done()

	expected := "transformed data from n1"
	if finalMsg != expected {
		t.Errorf("Expected %s, got %s", expected, finalMsg)
	}
}

func TestJob_SkipMiddleNode(t *testing.T) {
	d := NewDag()
	n1Executed := false
	n2Executed := false
	n3Executed := false
	mu := sync.Mutex{}

	f := func(name string, p *bool) NodeExecute {
		return func(ctx context.Context, message map[string]any) (map[string]any, error) {
			mu.Lock()
			*p = true
			mu.Unlock()
			return message, nil
		}
	}

	n1 := NewNode("n1", f("n1", &n1Executed))
	n2 := NewNode("n2", f("n2", &n2Executed))
	n3 := NewNode("n3", f("n3", &n3Executed))

	// n1 -> n2 (false)
	// n2 -> n3 (true)
	// Even if n2 -> n3 is true, if n2 is skipped, n3 should be skipped?
	// Or n3 executes with nil input?
	// According to my current logic: if n2 hasInput is false, it doesn't execute and its out-edges don't activate.
	d.AddEdge(n1, n2, func(message map[string]any) (map[string]any, bool) { return nil, false })
	d.AddEdge(n2, n3, func(message map[string]any) (map[string]any, bool) { return nil, true })

	job, _ := d.New(NewDefaultJob)
	job.Execute(nil)
	<-job.Done()

	if !n1Executed {
		t.Error("n1 should have executed")
	}
	if n2Executed {
		t.Error("n2 should have been skipped")
	}
	if n3Executed {
		t.Error("n3 should have been skipped because n2 was skipped")
	}
}

func TestJob_MultiInput(t *testing.T) {
	d := NewDag()
	var n3Input map[string]any
	mu := sync.Mutex{}

	n0 := NewNode("n0", func(ctx context.Context, message map[string]any) (map[string]any, error) {
		return message, nil
	})
	n1 := NewNode("n1", func(ctx context.Context, message map[string]any) (map[string]any, error) {
		return map[string]any{"v1": "n1"}, nil
	})
	n2 := NewNode("n2", func(ctx context.Context, message map[string]any) (map[string]any, error) {
		return map[string]any{"v2": "n2"}, nil
	})
	n3 := NewNode("n3", func(ctx context.Context, message map[string]any) (map[string]any, error) {
		mu.Lock()
		n3Input = maps.Clone(message)
		mu.Unlock()
		return nil, nil
	})

	d.AddEdge(n0, n1, func(message map[string]any) (map[string]any, bool) { return message, true })
	d.AddEdge(n0, n2, func(message map[string]any) (map[string]any, bool) { return message, true })
	d.AddEdge(n1, n3, func(message map[string]any) (map[string]any, bool) { return message, true })
	d.AddEdge(n2, n3, func(message map[string]any) (map[string]any, bool) { return message, true })

	job, err := d.New(NewDefaultJob)
	if err != nil {
		t.Fatalf("Failed to create job: %v", err)
	}
	job.Execute(nil)
	<-job.Done()

	// Multi-input should merge results
	if n3Input["v1"] != "n1" || n3Input["v2"] != "n2" {
		t.Errorf("Expected n3 to receive merged input from n1 and n2, got %v", n3Input)
	}
}

func TestJob_CancelWait(t *testing.T) {
	d := NewDag()
	var mu sync.Mutex
	runningCount := 0
	maxRunningCount := 0
	completedCount := 0

	f := func(ctx context.Context, message map[string]any) (map[string]any, error) {
		mu.Lock()
		runningCount++
		if runningCount > maxRunningCount {
			maxRunningCount = runningCount
		}
		mu.Unlock()
		defer func() {
			mu.Lock()
			runningCount--
			mu.Unlock()
		}()

		// Simulate work
		select {
		case <-time.After(200 * time.Millisecond):
		case <-ctx.Done():
			return nil, ctx.Err()
		}

		mu.Lock()
		completedCount++
		mu.Unlock()
		return nil, nil
	}

	n1 := NewNode("n1", f)
	n2 := NewNode("n2", f)
	n3 := NewNode("n3", f)

	// n1 -> n2, n1 -> n3
	d.AddEdge(n1, n2, func(message map[string]any) (map[string]any, bool) { return nil, true })
	d.AddEdge(n1, n3, func(message map[string]any) (map[string]any, bool) { return nil, true })

	job, _ := d.New(NewDefaultJob)
	job.Execute(nil)

	// Wait for n1 to start and finish, then n2/n3 to start
	time.Sleep(300 * time.Millisecond)

	mu.Lock()
	if runningCount == 0 {
		mu.Unlock()
		t.Fatal("Expected nodes to be running")
	}
	mu.Unlock()

	job.Cancel()

	// 立即检查是否还在运行。
	// 这里可能还是 > 0，因为 Cancel 刚被调用。
	// 但 Done() 不应该关闭。
	select {
	case <-job.Done():
		t.Fatal("Done should not be closed immediately after Cancel if nodes are still running")
	default:
		// OK
	}

	<-job.Done()

	mu.Lock()
	if runningCount != 0 {
		t.Errorf("Expected 0 running nodes after Cancel and Done, got %d", runningCount)
	}
	mu.Unlock()
}

func TestJob_EmptyDagCompletes(t *testing.T) {
	job, err := NewDag().New(nil)
	if err != nil {
		t.Fatalf("Failed to create empty job: %v", err)
	}
	if err := job.Execute(nil); err != nil {
		t.Fatalf("Failed to execute empty job: %v", err)
	}

	select {
	case <-job.Done():
	case <-time.After(time.Second):
		t.Fatal("Empty job did not complete")
	}
	if job.State() != JobStateFinished {
		t.Fatalf("Expected empty job to finish, got %v", job.State())
	}
}

func TestJob_CancelBeforeExecuteCompletes(t *testing.T) {
	job, err := NewDag().New(nil)
	if err != nil {
		t.Fatal(err)
	}
	job.Cancel()

	select {
	case <-job.Done():
	case <-time.After(time.Second):
		t.Fatal("Cancelled ready job did not complete")
	}
	if job.State() != JobStateCancelled {
		t.Fatalf("Expected cancelled state, got %v", job.State())
	}
	if err := job.Execute(nil); err == nil {
		t.Fatal("Expected executing a cancelled job to fail")
	}
}

func TestJob_NilEdgeIsUnconditional(t *testing.T) {
	d := NewDag()
	n1 := NewNode("start", func(ctx context.Context, message map[string]any) (map[string]any, error) {
		return map[string]any{"value": "passed"}, nil
	})
	var received string
	n2 := NewNode("end", func(ctx context.Context, message map[string]any) (map[string]any, error) {
		received = message["value"].(string)
		return nil, nil
	})
	d.AddEdge(n1, n2, nil)

	job, err := d.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := job.Execute(nil); err != nil {
		t.Fatal(err)
	}
	<-job.Done()
	if received != "passed" {
		t.Fatalf("Expected nil edge to pass data, got %q", received)
	}
}

func TestJob_ReportsNodeError(t *testing.T) {
	want := errors.New("node failed")
	d := NewDag()
	n1 := NewNode("start", func(ctx context.Context, message map[string]any) (map[string]any, error) {
		return nil, want
	})
	n2 := NewNode("end", nil)
	d.AddEdge(n1, n2, nil)

	job, err := d.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := job.Execute(nil); err != nil {
		t.Fatal(err)
	}
	<-job.Done()
	if !errors.Is(JobError(job), want) {
		t.Fatalf("Expected wrapped node error, got %v", JobError(job))
	}
	if job.State() != JobStateCancelled {
		t.Fatalf("Expected failed job to be cancelled, got %v", job.State())
	}
}

func TestJob_ConvertsPanicToError(t *testing.T) {
	d := NewDag()
	n1 := NewNode("start", func(ctx context.Context, message map[string]any) (map[string]any, error) {
		panic("boom")
	})
	n2 := NewNode("end", nil)
	d.AddEdge(n1, n2, nil)

	job, err := d.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := job.Execute(nil); err != nil {
		t.Fatal(err)
	}
	<-job.Done()
	if JobError(job) == nil || !strings.Contains(JobError(job).Error(), "boom") {
		t.Fatalf("Expected panic error, got %v", JobError(job))
	}
}

func TestJob_ConvertsEdgePanicToError(t *testing.T) {
	d := NewDag()
	n1 := NewNode("start", func(ctx context.Context, message map[string]any) (map[string]any, error) {
		return message, nil
	})
	n2 := NewNode("end", nil)
	d.AddEdge(n1, n2, func(message map[string]any) (map[string]any, bool) {
		panic("edge boom")
	})

	job, err := d.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := job.Execute(nil); err != nil {
		t.Fatal(err)
	}
	<-job.Done()
	if JobError(job) == nil || !strings.Contains(JobError(job).Error(), "edge boom") {
		t.Fatalf("Expected edge panic error, got %v", JobError(job))
	}
}

func TestJob_CancelWaitsForContextIgnoringNode(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	d := NewDag()
	n1 := NewNode("start", func(ctx context.Context, message map[string]any) (map[string]any, error) {
		close(started)
		<-release
		return nil, nil
	})
	n2 := NewNode("end", nil)
	d.AddEdge(n1, n2, nil)

	job, err := d.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := job.Execute(nil); err != nil {
		t.Fatal(err)
	}
	<-started
	job.Cancel()
	select {
	case <-job.Done():
		t.Fatal("Done closed before the running node returned")
	default:
	}
	close(release)
	select {
	case <-job.Done():
	case <-time.After(time.Second):
		t.Fatal("Cancelled job did not finish after its node returned")
	}
}

func TestJob_IsolatesBranchMessages(t *testing.T) {
	d := NewDag()
	n0 := NewNode("start", func(ctx context.Context, message map[string]any) (map[string]any, error) {
		return map[string]any{"value": "initial"}, nil
	})

	ready := sync.WaitGroup{}
	ready.Add(2)
	release := make(chan struct{})
	var first, second map[string]any
	n1 := NewNode("first", func(ctx context.Context, message map[string]any) (map[string]any, error) {
		ready.Done()
		<-release
		message["value"] = "first"
		first = message
		return nil, nil
	})
	n2 := NewNode("second", func(ctx context.Context, message map[string]any) (map[string]any, error) {
		ready.Done()
		<-release
		message["value"] = "second"
		second = message
		return nil, nil
	})
	d.AddEdge(n0, n1, nil)
	d.AddEdge(n0, n2, nil)

	job, err := d.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := job.Execute(nil); err != nil {
		t.Fatal(err)
	}
	ready.Wait()
	close(release)
	<-job.Done()

	if first["value"] != "first" || second["value"] != "second" {
		t.Fatalf("Branch messages were shared: first=%v second=%v", first, second)
	}
}
