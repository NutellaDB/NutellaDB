package btree

import "fmt"

func (bt *BTree) Find(key string) (interface{}, bool, error) {

	root, err := bt.loadNode(bt.RootID)
	if err != nil {
		return nil, false, fmt.Errorf("failed to load root node: %v", err)
	}

	return bt.findInNode(root, key)
}

func (bt *BTree) FindAll() []KeyValue {
	root, _ := bt.loadNode((bt.RootID))
	result := []KeyValue{}
	if root != nil {
		bt.findAllNodes(root, &result)
	}
	return result
}

func (bt *BTree) findAllNodes(node *Node, result *([]KeyValue)) {
	for i := range len(node.Keys) {
		*result = append(*result, node.Keys[i])
	}
	if node.IsLeaf {
		return
	}
	for i := range len(node.Children) {
		child, _ := bt.loadNode(node.Children[i])
		bt.findAllNodes(child, result)
	}
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
