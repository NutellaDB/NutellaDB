package cache

import (
	"db/database"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
)

var IS_IN_MEMORY = false

// false : FS cache
// true : in-memory cache

type Cache struct {
	CacheMap map[string]map[string]string `json:"cache_map"`
}

func (cache *Cache) writeCacheToFS(basepath string) error {
	cache_bytes, err := json.Marshal(cache)
	if err != nil {
		return err
	}
	err = os.WriteFile(filepath.Join(basepath, "cache.json"), cache_bytes, 0644)
	return err
}

func CreateCache(basepath string) (Cache, error) {
	db, err := database.LoadDatabase(basepath)
	if err != nil {
		log.Fatalf("error : %v", err)
	}

	collections, _ := db.GetAllCollections()
	fmt.Println(collections)

	cache := Cache{CacheMap: map[string]map[string]string{}}

	for i := range collections {
		cache.CacheMap[collections[i]] = map[string]string{}
	}

	if IS_IN_MEMORY {
		err = cache.writeCacheToFS(basepath)
		return Cache{}, err
	} else {
		return cache, nil
	}

}

func LoadCacheFromMemory(basepath string) (Cache, error) {
	cache_bytes, err := os.ReadFile(filepath.Join(basepath, "cache.json"))
	if err != nil {
		return Cache{}, err
	}

	var cache Cache
	err = json.Unmarshal(cache_bytes, &cache)
	if err != nil {
		return Cache{}, err
	}

	return cache, nil
}

// func (cache *Cache) SaveCache(basepath string)  error {

// }

func (cache *Cache) FindInCache(collection, key string) (string, error) {
	_, exists := cache.CacheMap[collection]
	if !exists {
		return "", fmt.Errorf("failed to find collection : %s", collection)
	}

	value, exists := cache.CacheMap[collection][key]
	if !exists {
		return "", fmt.Errorf("failed to find key : %s", key)
	}

	return value, nil
}

func FindInCacheMemory(basepath, collection, key string) (string, error) {
	cache, err := LoadCacheFromMemory(basepath)
	if err != nil {
		return "", err
	}

	value, err := cache.FindInCache(collection, key)
	return value, err
}

// func InsertInCache(collection, key, value string) error {

// }
