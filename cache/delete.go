package cache

import "fmt"

func (cache *Cache) DeleteFromCache(collection, key string) error {
	cache.Lock()
	defer cache.Unlock()

	collectionMap, exists := cache.CacheMap[collection]
	if !exists {
		return fmt.Errorf("failed to find collection '%s'", collection)
	}

	if element, found := collectionMap[key]; found {

		cache.LruList.Remove(element)

		delete(collectionMap, key)
		delete(cache.CacheData[collection], key)

		if len(collectionMap) == 0 {
			delete(cache.CacheMap, collection)
			delete(cache.CacheData, collection)
		}

		return nil
	}

	return fmt.Errorf("key '%s' not found in collection '%s'", key, collection)
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
