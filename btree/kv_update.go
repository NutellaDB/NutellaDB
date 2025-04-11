package btree

import "fmt"

func (bt *BTree) Update(key string, value interface{}) (bool, error) {

	root, err := bt.loadNode(bt.RootID)
	if err != nil {
		return false, fmt.Errorf("failed to load root node: %v", err)
	}

	return bt.updateInNode(root, key, value)
}

func (bt *BTree) updateInNode(node *Node, key string, value interface{}) (bool, error) {

	i := 0
	for i < len(node.Keys) && key > node.Keys[i].Key {
		i++
	}

	if i < len(node.Keys) && key == node.Keys[i].Key {

		node.Keys[i].Value = value
		return true, bt.saveNode(node)
	}

	if node.IsLeaf {
		return false, nil
	}

	child, err := bt.loadNode(node.Children[i])
	if err != nil {
		return false, fmt.Errorf("failed to load child node: %v", err)
	}

	return bt.updateInNode(child, key, value)
}
