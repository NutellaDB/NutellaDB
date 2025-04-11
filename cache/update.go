package cache

import "fmt"

func (cache *Cache) UpdateInCache(collection, key, value string) error {

	_, found := cache.get(collection, key)
	if !found {
		return fmt.Errorf("key '%s' not found in collection '%s'", key, collection)
	}

	return cache.set(collection, key, value)
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
