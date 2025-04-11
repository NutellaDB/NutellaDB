package cache

import "container/list"

func (cache *Cache) GetSize() int {
	cache.RLock()
	defer cache.RUnlock()

	size := 0
	for _, items := range cache.CacheMap {
		size += len(items)
	}

	return size
}

func (cache *Cache) GetMaxSize() int {
	return cache.MaxSize
}

func (cache *Cache) SetMaxSize(maxSize int) {
	cache.Lock()
	defer cache.Unlock()

	cache.MaxSize = maxSize

	totalItems := 0
	for _, colItems := range cache.CacheMap {
		totalItems += len(colItems)
	}

	for totalItems > cache.MaxSize {
		cache.evictLRU()
		totalItems--
	}
}

func (cache *Cache) Clear() {
	cache.Lock()
	defer cache.Unlock()

	cache.CacheMap = make(map[string]map[string]*list.Element)
	cache.CacheData = make(map[string]map[string]string)
	cache.LruList = list.New()
}

func (cache *Cache) GetAllKeys(collection string) []string {
	cache.RLock()
	defer cache.RUnlock()

	if collectionMap, exists := cache.CacheMap[collection]; exists {
		keys := make([]string, 0, len(collectionMap))
		for key := range collectionMap {
			keys = append(keys, key)
		}
		return keys
	}

	return []string{}
}

func (cache *Cache) GetAllCollections() []string {
	cache.RLock()
	defer cache.RUnlock()

	collections := make([]string, 0, len(cache.CacheMap))
	for collection := range cache.CacheMap {
		collections = append(collections, collection)
	}

	return collections
}
