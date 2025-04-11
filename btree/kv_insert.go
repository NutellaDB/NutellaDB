package btree

import "fmt"

func (bt *BTree) Insert(key string, value interface{}) error {

	root, err := bt.loadNode(bt.RootID)
	if err != nil {
		return fmt.Errorf("failed to load root node: %v", err)
	}

	if len(root.Keys) == 2*bt.Order-1 {

		newRoot := &Node{
			ID:       bt.allocateNodeID(),
			IsLeaf:   false,
			Keys:     []KeyValue{},
			Children: []int{root.ID},
		}

		bt.splitChild(newRoot, 0, root)

		bt.metadata.Lock()
		bt.RootID = newRoot.ID
		bt.metadata.Unlock()

		err = bt.saveNode(newRoot)
		if err != nil {
			return fmt.Errorf("failed to save new root node: %v", err)
		}
		err = bt.saveMetadata()
		if err != nil {
			return fmt.Errorf("failed to save metadata: %v", err)
		}

		return bt.insertNonFull(newRoot, key, value)
	}

	return bt.insertNonFull(root, key, value)
}

func (bt *BTree) splitChild(parent *Node, index int, child *Node) error {

	newChild := &Node{
		ID:       bt.allocateNodeID(),
		IsLeaf:   child.IsLeaf,
		Keys:     make([]KeyValue, bt.Order-1),
		Children: make([]int, 0),
	}

	copy(newChild.Keys, child.Keys[bt.Order:])

	if !child.IsLeaf {
		newChild.Children = make([]int, bt.Order)
		copy(newChild.Children, child.Children[bt.Order:])
		child.Children = child.Children[:bt.Order]
	}

	middleKey := child.Keys[bt.Order-1]

	child.Keys = child.Keys[:bt.Order-1]

	parent.Children = append(parent.Children, 0)
	copy(parent.Children[index+2:], parent.Children[index+1:])
	parent.Children[index+1] = newChild.ID

	parent.Keys = append(parent.Keys, KeyValue{})
	copy(parent.Keys[index+1:], parent.Keys[index:])
	parent.Keys[index] = middleKey

	err := bt.saveNode(parent)
	if err != nil {
		return fmt.Errorf("failed to save parent node: %v", err)
	}
	err = bt.saveNode(child)
	if err != nil {
		return fmt.Errorf("failed to save child node: %v", err)
	}
	err = bt.saveNode(newChild)
	if err != nil {
		return fmt.Errorf("failed to save new child node: %v", err)
	}

	return nil
}

func (bt *BTree) insertNonFull(node *Node, key string, value interface{}) error {

	i := len(node.Keys) - 1
	for i >= 0 && key < node.Keys[i].Key {
		i--
	}
	i++

	if i > 0 && i <= len(node.Keys) && node.Keys[i-1].Key == key {
		node.Keys[i-1].Value = value
		return bt.saveNode(node)
	}

	if node.IsLeaf {

		node.Keys = append(node.Keys, KeyValue{})
		copy(node.Keys[i+1:], node.Keys[i:])
		node.Keys[i] = KeyValue{Key: key, Value: value}

		return bt.saveNode(node)
	}

	child, err := bt.loadNode(node.Children[i])
	if err != nil {
		return fmt.Errorf("failed to load child node: %v", err)
	}

	if len(child.Keys) == 2*bt.Order-1 {
		err = bt.splitChild(node, i, child)
		if err != nil {
			return fmt.Errorf("failed to split child: %v", err)
		}

		if key > node.Keys[i].Key {
			child, err = bt.loadNode(node.Children[i+1])
			if err != nil {
				return fmt.Errorf("failed to load child node: %v", err)
			}
		} else if key == node.Keys[i].Key {

			node.Keys[i].Value = value
			return bt.saveNode(node)
		} else {

			child, err = bt.loadNode(node.Children[i])
			if err != nil {
				return fmt.Errorf("failed to load child node: %v", err)
			}
		}
	}

	return bt.insertNonFull(child, key, value)
}

func (bt *BTree) allocateNodeID() int {
	bt.metadata.Lock()
	defer bt.metadata.Unlock()

	id := bt.NextID
	bt.NextID++
	return id
}
