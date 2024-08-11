package trie_test

import (
	"testing"

	"ella.to/bus/internal/trie"
	"github.com/stretchr/testify/assert"
)

func TestTrie(t *testing.T) {
	tree := trie.New[int]()

	tree.Put("a.b.c", 1)
	tree.Put("a.b.*", 2)
	tree.Put("a.>", 3)

	values := tree.Get("a.b.c")

	assert.Len(t, values, 3)
	assert.Contains(t, values, 1)
	assert.Contains(t, values, 2)
	assert.Contains(t, values, 3)

	values = tree.Get("a.b.d")

	assert.Len(t, values, 2)
	assert.Contains(t, values, 2)
	assert.Contains(t, values, 3)

	tree.Del("a.b.c", 1, func(i1, i2 int) bool {
		return i1 == i2
	})

	values = tree.Get("a.b.c")

	assert.Len(t, values, 2)
	assert.Contains(t, values, 2)
	assert.Contains(t, values, 3)

}
