package cirque

import "sync"

type SyncMap[K comparable, V any] struct {
	holder map[K]V
	lock   sync.RWMutex
}

func NewSyncMap[K comparable, V any]() *SyncMap[K, V] {
	return &SyncMap[K, V]{
		holder: make(map[K]V),
		lock:   sync.RWMutex{},
	}
}

func (tm *SyncMap[K, V]) Set(key K, value V) {
	tm.lock.Lock()
	tm.holder[key] = value
	tm.lock.Unlock()
}

func (tm *SyncMap[K, V]) Get(key K) (value V, ok bool) {
	tm.lock.RLock()
	defer tm.lock.RUnlock()
	value, ok = tm.holder[key]
	return
}

func (tm *SyncMap[K, V]) Delete(key K) {
	tm.lock.Lock()
	delete(tm.holder, key)
	tm.lock.Unlock()
}
