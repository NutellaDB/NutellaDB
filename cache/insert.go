package cache

func (cache *Cache) InsertInCache(collection, key, value string) error {
	return cache.set(collection, key, value)
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

	return cache.SaveCache(basepath)
}
