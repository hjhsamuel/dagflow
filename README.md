# dagflow

`dagflow` is a lightweight, high-performance Go library for managing and executing Directed Acyclic Graph (DAG) workflows. It supports asynchronous execution, concurrent nodes, and conditional branching based on edge logic.

## Features

- **DAG Validation**: Built-in topological sort (Kahn's algorithm) to detect cycles and ensure graph validity (requires exactly one starting node).
- **Asynchronous Execution**: Job execution is non-blocking and managed via context.
- **Concurrency**: Automatically executes independent nodes in parallel to maximize performance.
- **Conditional Branching**: Use `EdgeFunc` to dynamically decide whether to activate downstream nodes based on upstream results.
- **Data Passing**: Seamlessly pass and transform data between nodes using `json.RawMessage`.
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
- `Execute(message json.RawMessage) (json.RawMessage, error)`

### Edge
The connection between nodes. It can optionally include an `EdgeFunc` to transform data or filter execution.
- `EdgeFunc func(message json.RawMessage) (json.RawMessage, bool)`

### Job
An instance of a `Dag` ready for execution. It manages the runtime state, concurrency, and lifecycle.

## Quick Start

### Basic Usage

```go
package main

import (
    "encoding/json"
    "fmt"
    "github.com/hjhsamuel/dagflow"
)

func main() {
    // 1. Create a new DAG
    dag := dagflow.NewDag()

    // 2. Define nodes
    n1 := dagflow.NewNode("Start", func(msg json.RawMessage) (json.RawMessage, error) {
        fmt.Println("Executing Start")
        return json.RawMessage(`{"data": "hello"}`), nil
    })

    n2 := dagflow.NewNode("End", func(msg json.RawMessage) (json.RawMessage, error) {
        fmt.Printf("Executing End with msg: %s\n", string(msg))
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
dag.AddEdge(n1, n2, func(msg json.RawMessage) (json.RawMessage, bool) {
    var data map[string]interface{}
    json.Unmarshal(msg, &data)
    
    // Only proceed if data is "valid"
    if data["status"] == "ok" {
        return msg, true
    }
    return nil, false
})
```
