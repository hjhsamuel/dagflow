package dagflow

import (
	"encoding/json"
	"sync"
	"testing"
	"time"
)

func TestDag_Check(t *testing.T) {
	d := NewDag()
	n1 := NewNode("n1", nil)
	n2 := NewNode("n2", nil)
	n3 := NewNode("n3", nil)

	d.AddEdge(n1, n2, nil)
	d.AddEdge(n2, n3, nil)

	if !d.Check() {
		t.Error("Expected DAG to be valid")
	}

	// Create cycle
	d.AddEdge(n3, n1, nil)
	if d.Check() {
		t.Error("Expected DAG to be invalid (cycle)")
	}
}

func TestJob_ExecuteAsync(t *testing.T) {
	d := NewDag()
	count := 0
	mu := sync.Mutex{}
	f := func(message json.RawMessage) (json.RawMessage, error) {
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
	d.AddEdge(n1, n2, func(message json.RawMessage) (json.RawMessage, bool) {
		return message, true
	})
	d.AddEdge(n1, n3, func(message json.RawMessage) (json.RawMessage, bool) {
		return nil, false
	})

	job, err := d.New(NewDefaultJob)
	if err != nil {
		t.Fatalf("Failed to create job: %v", err)
	}

	err = job.Execute(json.RawMessage(`"hello"`))
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
	f := func(message json.RawMessage) (json.RawMessage, error) {
		time.Sleep(100 * time.Millisecond)
		return nil, nil
	}

	n1 := NewNode("n1", f)
	n2 := NewNode("n2", f)
	n3 := NewNode("n3", f)

	// n1 -> n2
	// n1 -> n3
	// n2 and n3 should run in parallel
	d.AddEdge(n1, n2, func(message json.RawMessage) (json.RawMessage, bool) { return nil, true })
	d.AddEdge(n1, n3, func(message json.RawMessage) (json.RawMessage, bool) { return nil, true })

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

	n1 := NewNode("n1", func(message json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`"data from n1"`), nil
	})
	n2 := NewNode("n2", func(message json.RawMessage) (json.RawMessage, error) {
		mu.Lock()
		finalMsg = string(message)
		mu.Unlock()
		return nil, nil
	})

	d.AddEdge(n1, n2, func(message json.RawMessage) (json.RawMessage, bool) {
		// Transform message in edge
		msg := "transformed " + string(message)
		return json.RawMessage(msg), true
	})

	job, _ := d.New(NewDefaultJob)
	job.Execute(nil)
	<-job.Done()

	expected := `transformed "data from n1"`
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
		return func(message json.RawMessage) (json.RawMessage, error) {
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
	d.AddEdge(n1, n2, func(message json.RawMessage) (json.RawMessage, bool) { return nil, false })
	d.AddEdge(n2, n3, func(message json.RawMessage) (json.RawMessage, bool) { return nil, true })

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
	n3Input := ""
	mu := sync.Mutex{}

	n1 := NewNode("n1", func(message json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`"n1"`), nil
	})
	n2 := NewNode("n2", func(message json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`"n2"`), nil
	})
	n3 := NewNode("n3", func(message json.RawMessage) (json.RawMessage, error) {
		mu.Lock()
		n3Input = string(message)
		mu.Unlock()
		return nil, nil
	})

	d.AddEdge(n1, n3, func(message json.RawMessage) (json.RawMessage, bool) { return message, true })
	d.AddEdge(n2, n3, func(message json.RawMessage) (json.RawMessage, bool) { return message, true })

	job, _ := d.New(NewDefaultJob)
	job.Execute(nil)
	<-job.Done()

	// Current logic is last-one-wins for nodeResults.
	if n3Input != `"n1"` && n3Input != `"n2"` {
		t.Errorf("Expected n3 to receive input from n1 or n2, got %s", n3Input)
	}
}
