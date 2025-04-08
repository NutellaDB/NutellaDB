package btree

import "sync"

// KeyValue represents a key-value pair stored in the B-tree
type KeyValue struct {
	Key   string      `json:"key"`
	Value interface{} `json:"value"`
}

// Node represents a node in the B-tree
type Node struct {
	ID       int        `json:"id"`
	IsLeaf   bool       `json:"is_leaf"`
	Keys     []KeyValue `json:"keys"`
	Children []int      `json:"children"`
}

// BTree represents a B-tree
type BTree struct {
	RootID    int    `json:"root_id"`
	Order     int    `json:"order"`
	NextID    int    `json:"next_id"`
	DBID      string `json:"db_id"`
	PageDir   string `json:"page_dir"`
	metadata  *sync.RWMutex
	nodeCache map[int]*Node
}
