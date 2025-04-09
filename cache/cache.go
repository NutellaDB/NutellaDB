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
var MAX_CACHE_SIZE = 10

type Cache struct {
	CacheMap map[string]map[string]string `json:"cache_map"`
}

func (cache *Cache) SaveCache(basepath string) error {
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
		err = cache.SaveCache(basepath)
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

func (cache *Cache) InsertInCache(collection, key, value string) error {
	_, exists := cache.CacheMap[collection]
	if !exists {
		return fmt.Errorf("failed to find collection : %s", collection)
	}

	cache.CacheMap[collection][key] = value
	return nil
}

func InsertInCacheMemory(basepath, collection, key, value string) error {
	cache, err := LoadCacheFromMemory(basepath)
	if err != nil {
		return err
	}

	err = cache.InsertInCache(collection, key, value)
	if err != nil {
		return err
	}

	err = cache.SaveCache(basepath)
	return err
}

func (cache *Cache) UpdateInCache(collection, key, value string) error {
	return cache.InsertInCache(collection, key, value)
}

func UpdateCacheInMemory(basepath, collection, key, value string) error {
	cache, err := LoadCacheFromMemory(basepath)
	if err != nil {
		return err
	}

	err = cache.UpdateInCache(collection, key, value)
	if err != nil {
		return err
	}

	return cache.SaveCache(basepath)
}

func (cache *Cache) DeleteFromCache(collection, key string) error {
	_, exists := cache.CacheMap[collection]
	if !exists {
		return fmt.Errorf("failed to find collection : %s", collection)
	}

	delete(cache.CacheMap, key)
	return nil
}

func DeleteFromCacheMemory(basepath, collection, key string) error {
	cache, err := LoadCacheFromMemory(basepath)
	if err != nil {
		return err
	}

	err = cache.DeleteFromCache(collection, key)
	if err != nil {
		return err
	}

	return cache.SaveCache(basepath)
}
