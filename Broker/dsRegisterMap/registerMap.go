package dsRegisterMap

import (
	"sync"
)

type RegisterMap struct {
	Register map[string]string;
	mu 		 sync.RWMutex;
}

func NewRegisterMap() *RegisterMap {
	return &RegisterMap{
		Register: make(map[string]string),
	}
}

func (rm *RegisterMap) Add(key, value string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	rm.Register[key] = value
}

func (rm *RegisterMap) Get(key string) (string, bool) {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	value, exists := rm.Register[key]
	return value, exists
}

func (rm *RegisterMap) Exists(key string) bool {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	_, exists := rm.Register[key]
	return exists
}

func (rm *RegisterMap) Delete(key string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	delete(rm.Register, key)
}

func (rm *RegisterMap) GetAll() map[string]string {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	copy := make(map[string]string)
	for k, v := range rm.Register {
		copy[k] = v
	}
	return copy
}

