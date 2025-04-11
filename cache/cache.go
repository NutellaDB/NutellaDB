package cache

import (
	"container/list"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

var IS_IN_MEMORY = true
var MAX_CACHE_SIZE = 10

type CacheItem struct {
	Collection string
	Key        string
	Value      string
}

type Cache struct {
	sync.RWMutex
	CacheMap  map[string]map[string]*list.Element `json:"-"`
	LruList   *list.List                          `json:"-"`
	MaxSize   int                                 `json:"max_size"`
	CacheData map[string]map[string]string        `json:"cache_data"`
}

func NewCache(maxSize int) *Cache {
	return &Cache{
		CacheMap:  make(map[string]map[string]*list.Element),
		LruList:   list.New(),
		MaxSize:   maxSize,
		CacheData: make(map[string]map[string]string),
	}
}

func (cache *Cache) SaveCache(basepath string) error {
	cache.Lock()
	defer cache.Unlock()

	cache.CacheData = make(map[string]map[string]string)
	for collection, items := range cache.CacheMap {
		cache.CacheData[collection] = make(map[string]string)
		for key, element := range items {
			item := element.Value.(*CacheItem)
			cache.CacheData[collection][key] = item.Value
		}
	}

	cacheBytes, err := json.Marshal(cache)
	if err != nil {
		return err
	}

	err = os.WriteFile(filepath.Join(basepath, "cache.json"), cacheBytes, 0644)
	return err
}

func CreateCache(basepath string, collections []string) (*Cache, error) {
	fmt.Println(collections)

	cache := NewCache(MAX_CACHE_SIZE)

	for i := range collections {
		cache.CacheMap[collections[i]] = make(map[string]*list.Element)
		cache.CacheData[collections[i]] = make(map[string]string)
	}

	if IS_IN_MEMORY {
		err := cache.SaveCache(basepath)
		if err != nil {
			return nil, err
		}
		return cache, nil
	} else {
		return cache, nil
	}
}

func (cache *Cache) AddCollection(basepath, collectionName string) error {

	if _, exists := cache.CacheMap[collectionName]; exists {
		return fmt.Errorf("collection '%s' already exists", collectionName)
	}

	cache.CacheMap[collectionName] = make(map[string]*list.Element)
	cache.CacheData[collectionName] = make(map[string]string)

	if IS_IN_MEMORY {
		return cache.SaveCache(basepath)
	}

	return nil
}

func AddCollectionToMemory(basepath, collectionName string) error {
	cache, err := LoadCacheFromMemory(basepath)
	if err != nil {
		return err
	}

	err = cache.AddCollection(basepath, collectionName)
	if err != nil {
		return err
	}

	return cache.SaveCache(basepath)
}

func LoadCacheFromMemory(basepath string) (*Cache, error) {
	cacheBytes, err := os.ReadFile(filepath.Join(basepath, "cache.json"))
	if err != nil {
		return nil, err
	}

	var loadedCache struct {
		MaxSize   int                          `json:"max_size"`
		CacheData map[string]map[string]string `json:"cache_data"`
	}

	err = json.Unmarshal(cacheBytes, &loadedCache)
	if err != nil {
		return nil, err
	}

	cache := NewCache(loadedCache.MaxSize)

	for collection, items := range loadedCache.CacheData {
		cache.CacheMap[collection] = make(map[string]*list.Element)
		cache.CacheData[collection] = make(map[string]string)

		for key, value := range items {

			item := &CacheItem{
				Collection: collection,
				Key:        key,
				Value:      value,
			}

			element := cache.LruList.PushFront(item)

			cache.CacheMap[collection][key] = element
			cache.CacheData[collection][key] = value
		}
	}

	return cache, nil
}

func (cache *Cache) get(collection, key string) (*CacheItem, bool) {
	cache.RLock()
	defer cache.RUnlock()

	collectionMap, exists := cache.CacheMap[collection]
	if !exists {
		return nil, false
	}

	if element, exists := collectionMap[key]; exists {

		cache.LruList.MoveToFront(element)
		return element.Value.(*CacheItem), true
	}

	return nil, false
}

func (cache *Cache) set(collection, key, value string) error {
	cache.Lock()
	defer cache.Unlock()

	if _, exists := cache.CacheMap[collection]; !exists {
		cache.CacheMap[collection] = make(map[string]*list.Element)
		cache.CacheData[collection] = make(map[string]string)
	}

	if element, found := cache.CacheMap[collection][key]; found {

		cache.LruList.MoveToFront(element)
		item := element.Value.(*CacheItem)
		item.Value = value
		cache.CacheData[collection][key] = value
		return nil
	}

	item := &CacheItem{
		Collection: collection,
		Key:        key,
		Value:      value,
	}

	element := cache.LruList.PushFront(item)
	cache.CacheMap[collection][key] = element
	cache.CacheData[collection][key] = value

	totalItems := 0
	for _, colItems := range cache.CacheMap {
		totalItems += len(colItems)
	}

	if totalItems > cache.MaxSize {
		cache.evictLRU()
	}

	return nil
}

func (cache *Cache) evictLRU() {

	if element := cache.LruList.Back(); element != nil {
		item := element.Value.(*CacheItem)

		cache.LruList.Remove(element)

		delete(cache.CacheMap[item.Collection], item.Key)
		delete(cache.CacheData[item.Collection], item.Key)

		if len(cache.CacheMap[item.Collection]) == 0 {
			delete(cache.CacheMap, item.Collection)
			delete(cache.CacheData, item.Collection)
		}
	}
}
