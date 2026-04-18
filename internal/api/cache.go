package api

import (
	"log"
	"strings"
	"sync"
	"time"
)

const (
	ttlAnalytics  = 5 * time.Minute
	ttlRanking    = 2 * time.Minute
	ttlFundList   = 2 * time.Minute
	ttlFundDetail = 1 * time.Minute
	ttlHealth     = 10 * time.Second

	cleanupInterval = 5 * time.Minute
)

type cacheEntry struct {
	data      []byte
	expiresAt time.Time
}

// Cache is a thread-safe, TTL-based in-memory response cache.
type Cache struct {
	mu      sync.RWMutex
	entries map[string]cacheEntry

	hits   int64
	misses int64
}

func NewCache() *Cache {
	c := &Cache{entries: make(map[string]cacheEntry)}
	go c.cleanupLoop()
	return c
}

func (c *Cache) Get(key string) ([]byte, bool) {
	c.mu.RLock()
	e, ok := c.entries[key]
	c.mu.RUnlock()

	if !ok || time.Now().After(e.expiresAt) {
		c.mu.Lock()
		c.misses++
		c.mu.Unlock()
		return nil, false
	}

	c.mu.Lock()
	c.hits++
	c.mu.Unlock()
	return e.data, true
}

func (c *Cache) Set(key string, data []byte, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries[key] = cacheEntry{data: data, expiresAt: time.Now().Add(ttl)}
}

// InvalidatePrefix removes all entries whose key begins with prefix.
// Pass "" to flush everything.
func (c *Cache) InvalidatePrefix(prefix string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if prefix == "" {
		c.entries = make(map[string]cacheEntry)
		return
	}
	deleted := 0
	for k := range c.entries {
		if strings.HasPrefix(k, prefix) {
			delete(c.entries, k)
			deleted++
		}
	}
	if deleted > 0 {
		log.Printf("[cache] invalidated %d entry(ies) with prefix %q", deleted, prefix)
	}
}

func (c *Cache) Stats() (hits, misses int64, size int) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.hits, c.misses, len(c.entries)
}

func (c *Cache) cleanupLoop() {
	ticker := time.NewTicker(cleanupInterval)
	defer ticker.Stop()
	for range ticker.C {
		now := time.Now()
		c.mu.Lock()
		evicted := 0
		for k, e := range c.entries {
			if now.After(e.expiresAt) {
				delete(c.entries, k)
				evicted++
			}
		}
		c.mu.Unlock()
		if evicted > 0 {
			log.Printf("[cache] evicted %d expired entry(ies)", evicted)
		}
	}
}

