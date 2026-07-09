package dagflow

import (
	"encoding/json"

	"github.com/google/uuid"
)

type NodeItf interface {
	ID() string
	Name() string
	Execute(message json.RawMessage) (json.RawMessage, error)
}

type NodeExecute func(message json.RawMessage) (json.RawMessage, error)

type Node struct {
	id   string
	name string
	f    NodeExecute
}

func (n *Node) ID() string {
	return n.id
}

func (n *Node) Name() string {
	return n.name
}

func (n *Node) Execute(message json.RawMessage) (json.RawMessage, error) {
	if n.f != nil {
		return n.f(message)
	}
	return nil, nil
}

func NewNode(name string, f NodeExecute) NodeItf {
	return &Node{
		id:   uuid.NewString(),
		name: name,
		f:    f,
	}
}
