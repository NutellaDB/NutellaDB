package cache

import "fmt"

func (cache *Cache) FindInCache(collection, key string) (string, error) {
	item, found := cache.get(collection, key)
	if !found {
		return "", fmt.Errorf("failed to find key '%s' in collection '%s'", key, collection)
	}

	return item.Value, nil
}

func FindInCacheMemory(basepath, collection, key string) (string, error) {
	cache, err := LoadCacheFromMemory(basepath)
	if err != nil {
		return "", err
	}

	value, err := cache.FindInCache(collection, key)
	return value, err
}
