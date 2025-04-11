package btree

import (
	"fmt"
	"os"
)

func (bt *BTree) Delete(key string) (bool, error) {

	root, err := bt.loadNode(bt.RootID)
	if err != nil {

		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("failed to load root node: %v", err)
	}

	deleted, err := bt.deleteFromNode(root, key)
	if err != nil {
		return false, err
	}

	if len(root.Keys) == 0 && !root.IsLeaf && len(root.Children) > 0 {
		bt.metadata.Lock()
		oldRootID := bt.RootID
		bt.RootID = root.Children[0]
		bt.metadata.Unlock()

		err = bt.deleteNode(oldRootID)
		if err != nil && !os.IsNotExist(err) {
			return deleted, fmt.Errorf("failed to delete old root: %v", err)
		}

		err = bt.saveMetadata()
		if err != nil {
			return deleted, fmt.Errorf("failed to save metadata: %v", err)
		}
	}

	return deleted, nil
}

func (bt *BTree) loadNodeSafe(nodeID int) (*Node, error) {
	node, err := bt.loadNode(nodeID)
	if err != nil {
		if os.IsNotExist(err) {

			fmt.Printf("Warning: Node %d doesn't exist, ignoring\n", nodeID)
			return nil, nil
		}
		return nil, err
	}
	return node, nil
}

func (bt *BTree) nodeExists(nodeID int) bool {
	filename := bt.getNodeFilename(nodeID)
	_, err := os.Stat(filename)
	return !os.IsNotExist(err)
}

func (bt *BTree) getNodeFilename(nodeID int) string {
	return fmt.Sprintf("files/%s/pages/page_%d.json", bt.DBID, nodeID)
}

func (bt *BTree) deleteFromNode(node *Node, key string) (bool, error) {

	if node == nil {
		return false, nil
	}

	i := 0
	for i < len(node.Keys) && key > node.Keys[i].Key {
		i++
	}

	if i < len(node.Keys) && key == node.Keys[i].Key {

		if node.IsLeaf {

			node.Keys = append(node.Keys[:i], node.Keys[i+1:]...)
			return true, bt.saveNode(node)
		}

		if i >= len(node.Children) || !bt.nodeExists(node.Children[i]) {

			node.Keys = append(node.Keys[:i], node.Keys[i+1:]...)
			if i < len(node.Children) {
				node.Children = append(node.Children[:i], node.Children[i+1:]...)
			}
			return true, bt.saveNode(node)
		}

		pred, err := bt.getPredecessor(node, i)
		if err != nil {

			node.Keys = append(node.Keys[:i], node.Keys[i+1:]...)
			if i < len(node.Children) {
				node.Children = append(node.Children[:i], node.Children[i+1:]...)
			}
			return true, bt.saveNode(node)
		}

		node.Keys[i] = pred

		childNode, err := bt.loadNodeSafe(node.Children[i])
		if err != nil {
			return false, fmt.Errorf("failed to load child node: %v", err)
		}

		if childNode == nil {

			node.Keys = append(node.Keys[:i], node.Keys[i+1:]...)
			node.Children = append(node.Children[:i], node.Children[i+1:]...)
			return true, bt.saveNode(node)
		}

		if len(childNode.Keys) < bt.Order {
			err = bt.ensureMinKeys(node, i)
			if err != nil {

				fmt.Printf("Warning: Failed to ensure minimum keys: %v\n", err)
			}

			childNode, err = bt.loadNodeSafe(node.Children[i])
			if err != nil {
				return false, fmt.Errorf("failed to reload child node: %v", err)
			}

			if childNode == nil {

				return true, bt.saveNode(node)
			}
		}

		deleted, err := bt.deleteFromNode(childNode, pred.Key)
		if err != nil {
			return false, err
		}

		err = bt.saveNode(node)
		if err != nil {
			return deleted, fmt.Errorf("failed to save parent node: %v", err)
		}

		return deleted, nil
	}

	if node.IsLeaf {
		return false, nil
	}

	if i >= len(node.Children) {
		return false, nil
	}

	if !bt.nodeExists(node.Children[i]) {

		if i < len(node.Children) {
			node.Children = append(node.Children[:i], node.Children[i+1:]...)
		}

		if len(node.Children) <= i && i > 0 && len(node.Keys) >= i {
			node.Keys = node.Keys[:i-1]
		}
		return false, bt.saveNode(node)
	}

	childNode, err := bt.loadNodeSafe(node.Children[i])
	if err != nil {
		return false, fmt.Errorf("failed to load child node: %v", err)
	}

	if childNode == nil {

		node.Children = append(node.Children[:i], node.Children[i+1:]...)
		if len(node.Keys) > i {
			node.Keys = append(node.Keys[:i], node.Keys[i+1:]...)
		}
		return false, bt.saveNode(node)
	}

	if len(childNode.Keys) < bt.Order {
		err = bt.ensureMinKeys(node, i)
		if err != nil {

			fmt.Printf("Warning: Failed to ensure minimum keys: %v\n", err)
		}

		if i < len(node.Children) {
			childNode, err = bt.loadNodeSafe(node.Children[i])
			if err != nil {
				return false, fmt.Errorf("failed to reload child node: %v", err)
			}

			if childNode == nil {
				return false, bt.saveNode(node)
			}
		} else {

			return false, bt.saveNode(node)
		}
	}

	deleted, err := bt.deleteFromNode(childNode, key)
	if err != nil {
		return false, err
	}

	if deleted {
		err = bt.saveNode(node)
		if err != nil {
			return deleted, fmt.Errorf("failed to save parent node: %v", err)
		}
	}

	return deleted, nil
}

func (bt *BTree) getPredecessor(node *Node, index int) (KeyValue, error) {

	if index < 0 || index >= len(node.Children) {
		return KeyValue{}, fmt.Errorf("invalid index for getPredecessor")
	}

	childID := node.Children[index]

	if !bt.nodeExists(childID) {
		return KeyValue{}, fmt.Errorf("child node doesn't exist")
	}

	child, err := bt.loadNode(childID)
	if err != nil {
		return KeyValue{}, fmt.Errorf("failed to load child node: %v", err)
	}

	for !child.IsLeaf {
		if len(child.Children) == 0 {

			child.IsLeaf = true
			err = bt.saveNode(child)
			if err != nil {
				fmt.Printf("Warning: Failed to save fixed node: %v\n", err)
			}
			break
		}

		lastChildIndex := len(child.Children) - 1
		if lastChildIndex < 0 {
			return KeyValue{}, fmt.Errorf("invalid child index")
		}

		childID = child.Children[lastChildIndex]

		if !bt.nodeExists(childID) {

			if len(child.Keys) > 0 {
				return child.Keys[len(child.Keys)-1], nil
			}
			return KeyValue{}, fmt.Errorf("cannot find predecessor")
		}

		child, err = bt.loadNode(childID)
		if err != nil {
			return KeyValue{}, fmt.Errorf("failed to load child node: %v", err)
		}
	}

	if len(child.Keys) == 0 {
		return KeyValue{}, fmt.Errorf("leaf node has no keys")
	}

	return child.Keys[len(child.Keys)-1], nil
}

func (bt *BTree) ensureMinKeys(node *Node, index int) error {

	if index < 0 || index >= len(node.Children) {
		return fmt.Errorf("invalid index for ensureMinKeys")
	}

	if !bt.nodeExists(node.Children[index]) {

		node.Children = append(node.Children[:index], node.Children[index+1:]...)
		if len(node.Keys) > index {
			node.Keys = append(node.Keys[:index], node.Keys[index+1:]...)
		}
		return bt.saveNode(node)
	}

	child, err := bt.loadNode(node.Children[index])
	if err != nil {
		return fmt.Errorf("failed to load child node: %v", err)
	}

	if len(child.Keys) >= bt.Order {
		return nil
	}

	if index > 0 {

		if !bt.nodeExists(node.Children[index-1]) {

			node.Children = append(node.Children[:index-1], node.Children[index:]...)
			if len(node.Keys) > index-1 {
				node.Keys = append(node.Keys[:index-1], node.Keys[index:]...)
			}
			return bt.saveNode(node)
		}

		leftSibling, err := bt.loadNode(node.Children[index-1])
		if err != nil {
			return fmt.Errorf("failed to load left sibling: %v", err)
		}

		if len(leftSibling.Keys) >= bt.Order {

			child.Keys = append([]KeyValue{node.Keys[index-1]}, child.Keys...)

			if len(leftSibling.Keys) > 0 {
				node.Keys[index-1] = leftSibling.Keys[len(leftSibling.Keys)-1]
				leftSibling.Keys = leftSibling.Keys[:len(leftSibling.Keys)-1]
			}

			if !child.IsLeaf && len(leftSibling.Children) > 0 {
				child.Children = append([]int{leftSibling.Children[len(leftSibling.Children)-1]}, child.Children...)
				leftSibling.Children = leftSibling.Children[:len(leftSibling.Children)-1]
			}

			err = bt.saveNode(node)
			if err != nil {
				return fmt.Errorf("failed to save node: %v", err)
			}
			err = bt.saveNode(child)
			if err != nil {
				return fmt.Errorf("failed to save child: %v", err)
			}
			err = bt.saveNode(leftSibling)
			if err != nil {
				return fmt.Errorf("failed to save left sibling: %v", err)
			}

			return nil
		}
	}

	if index < len(node.Children)-1 {

		if !bt.nodeExists(node.Children[index+1]) {

			node.Children = append(node.Children[:index+1], node.Children[index+2:]...)
			if len(node.Keys) > index {
				node.Keys = append(node.Keys[:index], node.Keys[index+1:]...)
			}
			return bt.saveNode(node)
		}

		rightSibling, err := bt.loadNode(node.Children[index+1])
		if err != nil {
			return fmt.Errorf("failed to load right sibling: %v", err)
		}

		if len(rightSibling.Keys) >= bt.Order {

			child.Keys = append(child.Keys, node.Keys[index])

			if len(rightSibling.Keys) > 0 {
				node.Keys[index] = rightSibling.Keys[0]
				rightSibling.Keys = rightSibling.Keys[1:]
			}

			if !child.IsLeaf && len(rightSibling.Children) > 0 {
				child.Children = append(child.Children, rightSibling.Children[0])
				rightSibling.Children = rightSibling.Children[1:]
			}

			err = bt.saveNode(node)
			if err != nil {
				return fmt.Errorf("failed to save node: %v", err)
			}
			err = bt.saveNode(child)
			if err != nil {
				return fmt.Errorf("failed to save child: %v", err)
			}
			err = bt.saveNode(rightSibling)
			if err != nil {
				return fmt.Errorf("failed to save right sibling: %v", err)
			}

			return nil
		}
	}

	if index > 0 {

		if !bt.nodeExists(node.Children[index-1]) {

			if index < len(node.Children)-1 && bt.nodeExists(node.Children[index+1]) {
				rightSibling, err := bt.loadNode(node.Children[index+1])
				if err != nil {
					return fmt.Errorf("failed to load right sibling: %v", err)
				}
				return bt.mergeNodes(node, index, child, rightSibling)
			}

			node.Children = append(node.Children[:index-1], node.Children[index:]...)
			if len(node.Keys) > index-1 {
				node.Keys = append(node.Keys[:index-1], node.Keys[index:]...)
			}
			return bt.saveNode(node)
		}

		leftSibling, err := bt.loadNode(node.Children[index-1])
		if err != nil {
			return fmt.Errorf("failed to load left sibling: %v", err)
		}

		return bt.mergeNodes(node, index-1, leftSibling, child)
	} else {

		if index >= len(node.Children)-1 || !bt.nodeExists(node.Children[index+1]) {

			node.Children = append(node.Children[:index], node.Children[index+1:]...)
			if len(node.Keys) > index {
				node.Keys = append(node.Keys[:index], node.Keys[index+1:]...)
			}
			return bt.saveNode(node)
		}

		rightSibling, err := bt.loadNode(node.Children[index+1])
		if err != nil {
			return fmt.Errorf("failed to load right sibling: %v", err)
		}

		return bt.mergeNodes(node, index, child, rightSibling)
	}
}

func (bt *BTree) mergeNodes(parent *Node, index int, left *Node, right *Node) error {

	if left == nil || right == nil {
		return fmt.Errorf("cannot merge nil nodes")
	}

	if index < 0 || index >= len(parent.Keys) {
		return fmt.Errorf("invalid index for mergeNodes")
	}

	left.Keys = append(left.Keys, parent.Keys[index])

	parent.Keys = append(parent.Keys[:index], parent.Keys[index+1:]...)

	left.Keys = append(left.Keys, right.Keys...)

	if !left.IsLeaf && len(right.Children) > 0 {
		left.Children = append(left.Children, right.Children...)
	}

	rightChildIndex := index + 1
	if rightChildIndex < len(parent.Children) {
		parent.Children = append(parent.Children[:rightChildIndex], parent.Children[rightChildIndex+1:]...)
	} else if len(parent.Children) > rightChildIndex {
		parent.Children = parent.Children[:rightChildIndex]
	}

	err := bt.saveNode(parent)
	if err != nil {
		return fmt.Errorf("failed to save parent: %v", err)
	}
	err = bt.saveNode(left)
	if err != nil {
		return fmt.Errorf("failed to save left node: %v", err)
	}

	err = bt.deleteNode(right.ID)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete right node: %v", err)
	}

	return nil
}

func (bt *BTree) RepairTree() error {

	root, err := bt.loadNode(bt.RootID)
	if err != nil {
		if os.IsNotExist(err) {

			root = &Node{
				ID:       bt.RootID,
				IsLeaf:   true,
				Keys:     []KeyValue{},
				Children: []int{},
			}
			return bt.saveNode(root)
		}
		return fmt.Errorf("failed to load root node: %v", err)
	}

	return bt.repairNode(root)
}

func (bt *BTree) repairNode(node *Node) error {
	if node == nil {
		return nil
	}

	if !node.IsLeaf {
		validChildren := []int{}
		validKeys := []KeyValue{}

		keyIndex := 0

		for i, childID := range node.Children {
			if bt.nodeExists(childID) {
				validChildren = append(validChildren, childID)

				if i > 0 && keyIndex < len(node.Keys) {
					validKeys = append(validKeys, node.Keys[keyIndex])
					keyIndex++
				}

				child, err := bt.loadNode(childID)
				if err == nil {
					err = bt.repairNode(child)
					if err != nil {
						fmt.Printf("Warning: Failed to repair child %d: %v\n", childID, err)
					}
				}
			}
		}

		node.Children = validChildren

		if len(validChildren) > 0 {

			if len(validKeys) > len(validChildren)-1 {
				node.Keys = validKeys[:len(validChildren)-1]
			} else {
				node.Keys = validKeys
			}
		} else {
			node.Keys = []KeyValue{}
		}

		if len(node.Children) == 0 {
			node.IsLeaf = true
		}

		return bt.saveNode(node)
	}

	return bt.saveNode(node)
}
