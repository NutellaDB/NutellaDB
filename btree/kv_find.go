package btree

import "fmt"

// Find searches for a key in the B-tree and returns its value
func (bt *BTree) Find(key string) (interface{}, bool, error) {
	// Load root node
	root, err := bt.loadNode(bt.RootID)
	if err != nil {
		return nil, false, fmt.Errorf("failed to load root node: %v", err)
	}

	// Search for the key
	return bt.findInNode(root, key)
}

// findInNode searches for a key in a node and its children
func (bt *BTree) findInNode(node *Node, key string) (interface{}, bool, error) {
	// Find the index where the key should be
	i := 0
	for i < len(node.Keys) && key > node.Keys[i].Key {
		i++
	}

	// Check if we found the key
	if i < len(node.Keys) && key == node.Keys[i].Key {
		return node.Keys[i].Value, true, nil
	}

	// If this is a leaf node, the key doesn't exist
	if node.IsLeaf {
		return nil, false, nil
	}

	// Recursively search the appropriate child
	child, err := bt.loadNode(node.Children[i])
	if err != nil {
		return nil, false, fmt.Errorf("failed to load child node: %v", err)
	}

	return bt.findInNode(child, key)
}
