package main

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"log"
	"math"
	"net/http"
	"encoding/json"
	"sync"
)

// BloomFilter represents a Bloom filter
type BloomFilter struct {
	m     uint      // size of bit array
	k     uint      // number of hash functions
	bits  []bool    // bit array
	mu    sync.RWMutex // for thread safety
	items int       // count of items added
}

// NewBloomFilter creates a new Bloom filter with optimal size and hash count
func NewBloomFilter(expectedItems int, falsePositiveRate float64) *BloomFilter {
	m := uint(math.Ceil(-float64(expectedItems) * math.Log(falsePositiveRate) / math.Pow(math.Log(2), 2)))
	k := uint(math.Ceil(math.Log(2) * float64(m) / float64(expectedItems)))
	
	return &BloomFilter{
		m:    m,
		k:    k,
		bits: make([]bool, m),
	}
}

// Add adds an item to the Bloom filter
func (bf *BloomFilter) Add(item string) {
	bf.mu.Lock()
	defer bf.mu.Unlock()
	
	hashes := bf.getHashValues(item)
	for _, hash := range hashes {
		bf.bits[hash] = true
	}
	bf.items++
}

// Contains checks if an item might be in the Bloom filter
func (bf *BloomFilter) Contains(item string) bool {
	bf.mu.RLock()
	defer bf.mu.RUnlock()
	
	hashes := bf.getHashValues(item)
	for _, hash := range hashes {
		if !bf.bits[hash] {
			return false
		}
	}
	return true
}

// Stats returns statistics about the Bloom filter
func (bf *BloomFilter) Stats() map[string]interface{} {
	bf.mu.RLock()
	defer bf.mu.RUnlock()
	
	setBits := 0
	for _, bit := range bf.bits {
		if bit {
			setBits++
		}
	}
	
	// Calculate fill ratio and estimated false positive rate
	fillRatio := float64(setBits) / float64(bf.m)
	falsePositiveRate := math.Pow(fillRatio, float64(bf.k))
	
	return map[string]interface{}{
		"size":                bf.m,
		"hashFunctions":       bf.k,
		"itemsAdded":          bf.items,
		"bitsSet":             setBits,
		"fillRatio":           fillRatio,
		"falsePositiveRate":   falsePositiveRate,
	}
}

// Reset clears the Bloom filter
func (bf *BloomFilter) Reset() {
	bf.mu.Lock()
	defer bf.mu.Unlock()
	
	bf.bits = make([]bool, bf.m)
	bf.items = 0
}

// getHashValues generates k hash values for an item
func (bf *BloomFilter) getHashValues(item string) []uint {
	h := sha256.New()
	h.Write([]byte(item))
	hash := h.Sum(nil)
	
	result := make([]uint, bf.k)
	for i := uint(0); i < bf.k; i++ {
		// Use different parts of the hash for each hash function
		value := binary.BigEndian.Uint64(append(hash[:8], byte(i)))
		result[i] = uint(value % uint64(bf.m))
	}
	
	return result
}

// Global bloom filter instance with default size
var globalBloomFilter = NewBloomFilter(10000, 0.01)

func main() {
	http.HandleFunc("/api/add", handleAdd)
	http.HandleFunc("/api/check", handleCheck)
	http.HandleFunc("/api/stats", handleStats)
	http.HandleFunc("/api/reset", handleReset)
	
	fmt.Println("Bloom filter server running on :8081")
	log.Fatal(http.ListenAndServe(":8081", nil))
}

func handleAdd(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	
	var req struct {
		Item string `json:"item"`
	}
	
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}
	
	globalBloomFilter.Add(req.Item)
	
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"success": true})
}

func handleCheck(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	
	var req struct {
		Item string `json:"item"`
	}
	
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}
	
	contains := globalBloomFilter.Contains(req.Item)
	
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"exists": contains})
}

func handleStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	
	stats := globalBloomFilter.Stats()
	
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}

func handleReset(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	
	globalBloomFilter.Reset()
	
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"success": true})
}