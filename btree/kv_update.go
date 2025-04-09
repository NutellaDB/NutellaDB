package btree

import "fmt"

// Update updates the value of a key in the B-tree
func (bt *BTree) Update(key string, value interface{}) (bool, error) {
	// Load root node
	root, err := bt.loadNode(bt.RootID)
	if err != nil {
		return false, fmt.Errorf("failed to load root node: %v", err)
	}

	// Update the key
	return bt.updateInNode(root, key, value)
}

// updateInNode updates the value of a key in a node and its children
func (bt *BTree) updateInNode(node *Node, key string, value interface{}) (bool, error) {
	// Find the index where the key should be
	i := 0
	for i < len(node.Keys) && key > node.Keys[i].Key {
		i++
	}

	// Check if we found the key
	if i < len(node.Keys) && key == node.Keys[i].Key {
		// Update the value
		node.Keys[i].Value = value
		return true, bt.saveNode(node)
	}

	// If this is a leaf node, the key doesn't exist
	if node.IsLeaf {
		return false, nil
	}

	// Recursively update the appropriate child
	child, err := bt.loadNode(node.Children[i])
	if err != nil {
		return false, fmt.Errorf("failed to load child node: %v", err)
	}

	return bt.updateInNode(child, key, value)
}
