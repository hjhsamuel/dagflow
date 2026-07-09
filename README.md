# dagflow

`dagflow` is a lightweight, high-performance Go library for managing and executing Directed Acyclic Graph (DAG) workflows. It supports asynchronous execution, concurrent nodes, and conditional branching based on edge logic.

## Features

- **DAG Validation**: Built-in topological sort (Kahn's algorithm) to detect cycles and ensure graph validity (requires exactly one starting node).
- **Asynchronous Execution**: Job execution is non-blocking and managed via context.
- **Concurrency**: Automatically executes independent nodes in parallel to maximize performance.
- **Conditional Branching**: Use `EdgeFunc` to dynamically decide whether to activate downstream nodes based on upstream results.
- **Data Passing & Merging**: Seamlessly pass and merge data between nodes using `map[string]any`.
- **Thread-Safe**: Safe for concurrent use with proper state management.

## Installation

```bash
go get github.com/hjhsamuel/dagflow
```

## Core Components

### Dag
The structure used to define the workflow. You add nodes and edges to build your graph.

### Node
The unit of work. Each node has a unique ID, a name, and an execution function.
- `Execute(ctx context.Context, message map[string]any) (map[string]any, error)`

### Edge
The connection between nodes. It can optionally include an `EdgeFunc` to transform data or filter execution.
- `EdgeFunc func(message map[string]any) (map[string]any, bool)`

### Job
An instance of a `Dag` ready for execution. It manages the runtime state, concurrency, and lifecycle.

## Data Merging & Field Conflicts

When a node has multiple incoming edges (multiple parent nodes), the data from all activated upstream edges will be merged into the input for the downstream node.

**Note on Starting Node**: The input provided to `job.Execute(initialMsg)` will be passed to the starting node(s) of the DAG.

### Merging Logic
- The library uses `maps.Copy` to merge results from multiple edges.
- **Important**: If multiple upstream nodes or edges provide the same key in their result maps, the values will be overwritten. The final value depends on the completion order of the upstream nodes.
- **Data Isolation**: Each `Node` and `EdgeFunc` should ideally return a new map or be aware that the input map is shared among concurrent downstream edges to prevent race conditions or unexpected side effects.

### Avoiding Field Conflicts
- **Recommendation**: To avoid unexpected behavior, ensure that different upstream nodes use unique keys or carefully design the `EdgeFunc` to handle potential field overlaps.
- **Prefixing**: A common strategy is to prefix keys with the node name or ID (e.g., `{"node1.status": "ok", "node2.status": "error"}`).
- **Edge Transformation**: Use `EdgeFunc` to rename fields before they reach the downstream node.

## Error Handling & Cancellation

- If a `Node` returns an error, the `Job` will be cancelled automatically.
- Use `job.Cancel()` to manually stop the execution.
- Context is propagated to all nodes, allowing for graceful shutdown of long-running tasks.

## Quick Start

### Basic Usage

```go
package main

import (
    "context"
    "fmt"
    "github.com/hjhsamuel/dagflow"
)

func main() {
    // 1. Create a new DAG
    dag := dagflow.NewDag()

    // 2. Define nodes
    n1 := dagflow.NewNode("Start", func(ctx context.Context, msg map[string]any) (map[string]any, error) {
        fmt.Println("Executing Start")
        return map[string]any{"data": "hello"}, nil
    })

    n2 := dagflow.NewNode("End", func(ctx context.Context, msg map[string]any) (map[string]any, error) {
        fmt.Printf("Executing End with msg: %v\n", msg)
        return nil, nil
    })

    // 3. Add edges
    dag.AddEdge(n1, n2, nil)

    // 4. Create and execute a Job
    job, _ := dag.New(nil)
    job.Execute(nil)

    // Wait for completion
    <-job.Done()
}
```

### Conditional Execution

You can control the flow by providing an `EdgeFunc`. If it returns `false`, the downstream node will not be triggered.

```go
dag.AddEdge(n1, n2, func(msg map[string]any) (map[string]any, bool) {
    // Only proceed if status is "ok"
    if msg["status"] == "ok" {
        return msg, true
    }
    return nil, false
})
```
