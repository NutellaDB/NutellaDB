package btree

import (
	"os"
	"path/filepath"
)

func ListDatabases(root string) ([]string, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		// If the folder doesn't exist yet, treat that as "no databases"
		if os.IsNotExist(err) {
			return []string{}, nil
		}
		return nil, err
	}

	var dbIDs []string
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}

		manifest := filepath.Join(root, e.Name(), "manifest.json")
		if _, err := os.Stat(manifest); err == nil {
			dbIDs = append(dbIDs, e.Name())
		}
	}
	return dbIDs, nil
}
