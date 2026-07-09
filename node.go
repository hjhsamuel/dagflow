package dagflow

import (
	"context"

	"github.com/google/uuid"
)

type NodeItf interface {
	ID() string
	Name() string
	Execute(ctx context.Context, message map[string]any) (map[string]any, error)
}

type NodeExecute func(ctx context.Context, message map[string]any) (map[string]any, error)

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

func (n *Node) Execute(ctx context.Context, message map[string]any) (map[string]any, error) {
	if n.f != nil {
		return n.f(ctx, message)
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
