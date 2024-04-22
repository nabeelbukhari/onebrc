package main

type entry struct {
	key uint64
	mid int
}

const (
	// FNV-1a
	offset64 = uint64(14695981039346656037)
	prime64  = uint64(1099511628211)

	// Init64 is what 64 bits hash values should be initialized with.
	Init64 = offset64

	// use power of 2 for fast modulo calculation
	nBuckets = 1 << 12
)

type (
	Map[K string | []byte, V any] struct {
		buckets [][]entry
		cache   []V
	}
)

func NewHashMap[K string, T any](size uint64) *Map[K, T] {
	buckets := make([][]entry, nBuckets)
	cache := make([]T, 0, size)

	return &Map[K, T]{buckets: buckets, cache: cache}
}

func (m *Map[K, V]) Get(key string) (V, bool) {
	hash := HashString64(key)
	i := hash & uint64(nBuckets-1)
	for j := 0; j < len(m.buckets[i]); j++ {
		e := &m.buckets[i][j]
		if e.key == hash {
			return m.cache[e.mid], true
		}
	}
	return *new(V), false
}

func (m *Map[K, V]) GetUsingHash(hash uint64) (V, bool) {
	i := hash & uint64(nBuckets-1)
	for j := 0; j < len(m.buckets[i]); j++ {
		e := &m.buckets[i][j]
		if e.key == hash {
			return m.cache[e.mid], true
		}
	}
	return *new(V), false
}

func (m *Map[K, V]) SetUsingHash(hash uint64, value V) {
	i := hash & uint64(nBuckets-1)
	m.buckets[i] = append(m.buckets[i], entry{key: hash, mid: len(m.cache)})
	m.cache = append(m.cache, value)
}

func (m *Map[K, V]) SetBytes(key []byte, value V) {
	hash := HashBytes64(key)
	i := hash & uint64(nBuckets-1)
	m.buckets[i] = append(m.buckets[i], entry{key: hash, mid: len(m.cache)})
	m.cache = append(m.cache, value)
}

// HashString64 returns the hash of s.
func HashString64(s string) uint64 {
	return AddString64(Init64, s)
}

// HashBytes64 returns the hash of u.
func HashBytes64(b []byte) uint64 {
	return AddBytes64(Init64, b)
}

// HashUint64 returns the hash of u.
func HashUint64(u uint64) uint64 {
	return AddUint64(Init64, u)
}

// AddString64 adds the hash of s to the precomputed hash value h.
func AddString64(h uint64, s string) uint64 {
	if len(s) >= 16 {
		for _, c := range s {
			h = (h ^ uint64(c)) * prime64
		}
		s = s[16:]
	}

	for len(s) >= 8 {
		h = (h ^ uint64(s[0])) * prime64
		h = (h ^ uint64(s[1])) * prime64
		h = (h ^ uint64(s[2])) * prime64
		h = (h ^ uint64(s[3])) * prime64
		h = (h ^ uint64(s[4])) * prime64
		h = (h ^ uint64(s[5])) * prime64
		h = (h ^ uint64(s[6])) * prime64
		h = (h ^ uint64(s[7])) * prime64
		s = s[8:]
	}

	if len(s) >= 4 {
		h = (h ^ uint64(s[0])) * prime64
		h = (h ^ uint64(s[1])) * prime64
		h = (h ^ uint64(s[2])) * prime64
		h = (h ^ uint64(s[3])) * prime64
		s = s[4:]
	}

	if len(s) >= 2 {
		h = (h ^ uint64(s[0])) * prime64
		h = (h ^ uint64(s[1])) * prime64
		s = s[2:]
	}

	if len(s) > 0 {
		h = (h ^ uint64(s[0])) * prime64
	}

	return h
}

// AddBytes64 adds the hash of b to the precomputed hash value h.
func AddBytes64(h uint64, b []byte) uint64 {
	if len(b) >= 16 {
		for _, c := range b {
			h = (h ^ uint64(c)) * prime64
		}
		b = b[16:]
	}

	for len(b) >= 8 {
		h = (h ^ uint64(b[0])) * prime64
		h = (h ^ uint64(b[1])) * prime64
		h = (h ^ uint64(b[2])) * prime64
		h = (h ^ uint64(b[3])) * prime64
		h = (h ^ uint64(b[4])) * prime64
		h = (h ^ uint64(b[5])) * prime64
		h = (h ^ uint64(b[6])) * prime64
		h = (h ^ uint64(b[7])) * prime64
		b = b[8:]
	}

	if len(b) >= 4 {
		h = (h ^ uint64(b[0])) * prime64
		h = (h ^ uint64(b[1])) * prime64
		h = (h ^ uint64(b[2])) * prime64
		h = (h ^ uint64(b[3])) * prime64
		b = b[4:]
	}

	if len(b) >= 2 {
		h = (h ^ uint64(b[0])) * prime64
		h = (h ^ uint64(b[1])) * prime64
		b = b[2:]
	}

	if len(b) > 0 {
		h = (h ^ uint64(b[0])) * prime64
	}

	return h
}

// AddUint64 adds the hash value of the 8 bytes of u to h.
func AddUint64(h uint64, u uint64) uint64 {
	h = (h ^ ((u >> 56) & 0xFF)) * prime64
	h = (h ^ ((u >> 48) & 0xFF)) * prime64
	h = (h ^ ((u >> 40) & 0xFF)) * prime64
	h = (h ^ ((u >> 32) & 0xFF)) * prime64
	h = (h ^ ((u >> 24) & 0xFF)) * prime64
	h = (h ^ ((u >> 16) & 0xFF)) * prime64
	h = (h ^ ((u >> 8) & 0xFF)) * prime64
	h = (h ^ ((u >> 0) & 0xFF)) * prime64
	return h
}
