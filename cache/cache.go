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

// CacheItem represents each item in the cache
type CacheItem struct {
	Collection string
	Key        string
	Value      string
}

// Cache is our LRU cache implementation
type Cache struct {
	sync.RWMutex
	CacheMap  map[string]map[string]*list.Element `json:"-"` // Maps collection -> key -> list element
	LruList   *list.List                          `json:"-"` // Doubly linked list for LRU ordering
	MaxSize   int                                 `json:"max_size"`
	CacheData map[string]map[string]string        `json:"cache_data"` // For JSON serialization
}

// NewCache creates a new LRU cache with the given max size
func NewCache(maxSize int) *Cache {
	return &Cache{
		CacheMap:  make(map[string]map[string]*list.Element),
		LruList:   list.New(),
		MaxSize:   maxSize,
		CacheData: make(map[string]map[string]string),
	}
}

// SaveCache persists the cache to disk
func (cache *Cache) SaveCache(basepath string) error {
	cache.Lock()
	defer cache.Unlock()

	// Update serializable cache data from the CacheMap
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

// CreateCache initializes a cache, either in memory or from disk
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

// AddCollection adds a new collection to the cache and saves it to memory
func (cache *Cache) AddCollection(basepath, collectionName string) error {

	// Check if collection already exists
	if _, exists := cache.CacheMap[collectionName]; exists {
		return fmt.Errorf("collection '%s' already exists", collectionName)
	}

	// Initialize the collection maps
	cache.CacheMap[collectionName] = make(map[string]*list.Element)
	cache.CacheData[collectionName] = make(map[string]string)

	// If we're using persistent storage, save the updated cache
	if IS_IN_MEMORY {
		return cache.SaveCache(basepath)
	}

	return nil
}

// AddCollectionToMemory creates a new collection in the cache loaded from disk
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

// LoadCacheFromMemory loads the cache from disk
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

	// Create a new cache with the loaded max size
	cache := NewCache(loadedCache.MaxSize)

	// Populate the cache with the loaded data
	for collection, items := range loadedCache.CacheData {
		cache.CacheMap[collection] = make(map[string]*list.Element)
		cache.CacheData[collection] = make(map[string]string)

		for key, value := range items {
			// Create a new item
			item := &CacheItem{
				Collection: collection,
				Key:        key,
				Value:      value,
			}

			// Add it to the LRU list
			element := cache.LruList.PushFront(item)

			// Update the cache map
			cache.CacheMap[collection][key] = element
			cache.CacheData[collection][key] = value
		}
	}

	return cache, nil
}

// get retrieves a value from cache and updates its position in the LRU list
func (cache *Cache) get(collection, key string) (*CacheItem, bool) {
	cache.RLock()
	defer cache.RUnlock()

	collectionMap, exists := cache.CacheMap[collection]
	if !exists {
		return nil, false
	}

	if element, exists := collectionMap[key]; exists {
		// Move this element to the front of the list (mark as most recently used)
		cache.LruList.MoveToFront(element)
		return element.Value.(*CacheItem), true
	}

	return nil, false
}

// FindInCache retrieves an item from the cache
func (cache *Cache) FindInCache(collection, key string) (string, error) {
	item, found := cache.get(collection, key)
	if !found {
		return "", fmt.Errorf("failed to find key '%s' in collection '%s'", key, collection)
	}

	return item.Value, nil
}

// FindInCacheMemory loads the cache from disk and finds a value
func FindInCacheMemory(basepath, collection, key string) (string, error) {
	cache, err := LoadCacheFromMemory(basepath)
	if err != nil {
		return "", err
	}

	value, err := cache.FindInCache(collection, key)
	return value, err
}

// set adds or updates an item in the cache and manages LRU eviction
func (cache *Cache) set(collection, key, value string) error {
	cache.Lock()
	defer cache.Unlock()

	// Ensure collection exists in cache
	if _, exists := cache.CacheMap[collection]; !exists {
		cache.CacheMap[collection] = make(map[string]*list.Element)
		cache.CacheData[collection] = make(map[string]string)
	}

	// Check if key already exists in cache
	if element, found := cache.CacheMap[collection][key]; found {
		// Update existing item
		cache.LruList.MoveToFront(element)
		item := element.Value.(*CacheItem)
		item.Value = value
		cache.CacheData[collection][key] = value
		return nil
	}

	// Create new cache item
	item := &CacheItem{
		Collection: collection,
		Key:        key,
		Value:      value,
	}

	// Add to front of LRU list
	element := cache.LruList.PushFront(item)
	cache.CacheMap[collection][key] = element
	cache.CacheData[collection][key] = value

	// Check if we need to evict the least recently used item
	totalItems := 0
	for _, colItems := range cache.CacheMap {
		totalItems += len(colItems)
	}

	if totalItems > cache.MaxSize {
		cache.evictLRU()
	}

	return nil
}

// evictLRU removes the least recently used item from the cache
func (cache *Cache) evictLRU() {
	// Get the back of the list (least recently used)
	if element := cache.LruList.Back(); element != nil {
		item := element.Value.(*CacheItem)

		// Remove from the list
		cache.LruList.Remove(element)

		// Remove from the maps
		delete(cache.CacheMap[item.Collection], item.Key)
		delete(cache.CacheData[item.Collection], item.Key)

		// If the collection is now empty, clean it up
		if len(cache.CacheMap[item.Collection]) == 0 {
			delete(cache.CacheMap, item.Collection)
			delete(cache.CacheData, item.Collection)
		}
	}
}

// InsertInCache adds or updates an item in the cache
func (cache *Cache) InsertInCache(collection, key, value string) error {
	return cache.set(collection, key, value)
}

// InsertInCacheMemory loads the cache, inserts a value, and saves it back
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

// UpdateInCache updates an existing item in the cache
func (cache *Cache) UpdateInCache(collection, key, value string) error {
	// Check if the item exists
	_, found := cache.get(collection, key)
	if !found {
		return fmt.Errorf("key '%s' not found in collection '%s'", key, collection)
	}

	// Use set to update the value and manage LRU
	return cache.set(collection, key, value)
}

// UpdateCacheInMemory loads the cache, updates a value, and saves it back
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

// DeleteFromCache removes an item from the cache
func (cache *Cache) DeleteFromCache(collection, key string) error {
	cache.Lock()
	defer cache.Unlock()

	collectionMap, exists := cache.CacheMap[collection]
	if !exists {
		return fmt.Errorf("failed to find collection '%s'", collection)
	}

	if element, found := collectionMap[key]; found {
		// Remove from LRU list
		cache.LruList.Remove(element)

		// Remove from maps
		delete(collectionMap, key)
		delete(cache.CacheData[collection], key)

		// If collection is now empty, clean it up
		if len(collectionMap) == 0 {
			delete(cache.CacheMap, collection)
			delete(cache.CacheData, collection)
		}

		return nil
	}

	return fmt.Errorf("key '%s' not found in collection '%s'", key, collection)
}

// DeleteFromCacheMemory loads the cache, deletes a value, and saves it back
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

// GetSize returns the current number of items in the cache
func (cache *Cache) GetSize() int {
	cache.RLock()
	defer cache.RUnlock()

	size := 0
	for _, items := range cache.CacheMap {
		size += len(items)
	}

	return size
}

// GetMaxSize returns the maximum cache size
func (cache *Cache) GetMaxSize() int {
	return cache.MaxSize
}

// SetMaxSize updates the maximum cache size and evicts items if necessary
func (cache *Cache) SetMaxSize(maxSize int) {
	cache.Lock()
	defer cache.Unlock()

	cache.MaxSize = maxSize

	// Evict items if needed
	totalItems := 0
	for _, colItems := range cache.CacheMap {
		totalItems += len(colItems)
	}

	for totalItems > cache.MaxSize {
		cache.evictLRU()
		totalItems--
	}
}

// Clear empties the cache
func (cache *Cache) Clear() {
	cache.Lock()
	defer cache.Unlock()

	cache.CacheMap = make(map[string]map[string]*list.Element)
	cache.CacheData = make(map[string]map[string]string)
	cache.LruList = list.New()
}

// GetAllKeys returns all keys in a collection
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

// GetAllCollections returns all collection names
func (cache *Cache) GetAllCollections() []string {
	cache.RLock()
	defer cache.RUnlock()

	collections := make([]string, 0, len(cache.CacheMap))
	for collection := range cache.CacheMap {
		collections = append(collections, collection)
	}

	return collections
}
