package trie

import (
	"strings"
)

type Node[T any] struct {
	children map[string]*Node[T]
	values   []T
	eq       func(a, b T) bool
}

// New creates a new Node instance.
func New[T any](eq func(a, b T) bool) *Node[T] {
	return &Node[T]{
		children: make(map[string]*Node[T]),
		values:   []T{},
		eq:       eq,
	}
}

// Put inserts a key with a value into the trie-like structure with pattern matching.
func (n *Node[T]) Put(key string, val T) {
	segments := strings.Split(key, ".")
	node := n

	for _, segment := range segments {
		if node.children[segment] == nil {
			node.children[segment] = New[T](n.eq)
		}
		node = node.children[segment]
	}

	for _, v := range node.values {
		if n.eq(v, val) {
			return
		}
	}

	node.values = append(node.values, val)
}

// Get retrieves all values that match the concrete key.
func (n *Node[T]) Get(key string) []T {
	segments := strings.Split(key, ".")
	var results []T

	var search func(node *Node[T], segIdx int)
	search = func(node *Node[T], segIdx int) {
		if segIdx == len(segments) {
			results = append(results, node.values...)
			return
		}

		segment := segments[segIdx]

		// Match the exact segment
		if child, exists := node.children[segment]; exists {
			search(child, segIdx+1)
		}

		// Match '*'
		if child, exists := node.children["*"]; exists {
			search(child, segIdx+1)
		}

		// Match '>'
		if child, exists := node.children[">"]; exists {
			results = append(results, child.values...)
		}
	}

	search(n, 0)

	return results
}

// Del removes a value from the trie-like structure using a custom equality function and deletes paths if they become empty.
func (n *Node[T]) Del(key string, val T) {
	segments := strings.Split(key, ".")
	node := n
	var path []*Node[T]

	for _, segment := range segments {
		if node.children[segment] == nil {
			return
		}
		path = append(path, node)
		node = node.children[segment]
	}

	// Remove the value from the node's values slice using the provided equality function
	for i, v := range node.values {
		if n.eq(v, val) {
			node.values = append(node.values[:i], node.values[i+1:]...)
			break
		}
	}

	// Clean up the trie if there are no more values in this path
	for i := len(path) - 1; i >= 0; i-- {
		if len(node.values) == 0 && len(node.children) == 0 {
			delete(path[i].children, segments[i])
			node = path[i]
		} else {
			break
		}
	}
}
