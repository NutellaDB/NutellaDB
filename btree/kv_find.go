package btree

import "fmt"

func (bt *BTree) Find(key string) (interface{}, bool, error) {

	root, err := bt.loadNode(bt.RootID)
	if err != nil {
		return nil, false, fmt.Errorf("failed to load root node: %v", err)
	}

	return bt.findInNode(root, key)
}

func (bt *BTree) findInNode(node *Node, key string) (interface{}, bool, error) {

	i := 0
	for i < len(node.Keys) && key > node.Keys[i].Key {
		i++
	}

	if i < len(node.Keys) && key == node.Keys[i].Key {
		return node.Keys[i].Value, true, nil
	}

	if node.IsLeaf {
		return nil, false, nil
	}

	child, err := bt.loadNode(node.Children[i])
	if err != nil {
		return nil, false, fmt.Errorf("failed to load child node: %v", err)
	}

	return bt.findInNode(child, key)
}
