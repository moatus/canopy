package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"runtime"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
)

// seekltPebbleSync controls fsync behavior for SeekLT commits in this package.
// Default true to mirror production parity; benchmarks may set PEBBLE_SYNC env
// or evaluate durability trade-offs separately. We keep this separate from
// benchmark_test.go's variable to avoid cross-file coupling in non-test builds.
var seekltPebbleSync = true

/* seeklt_pebble_store.go - SeekLT Pebble implementation (Issue-196 style) adapted for pebble-test */

// SeekLT Pebble key layout: [UserKey][8-byte InvertedVersion]
// SeekLT Pebble value layout: [1-byte Tombstone][ActualValue]
// InvertedVersion = ^version to make newer versions sort first lexicographically

const (
	SeekLTVersionSize    = 8
	SeekLTDeadTombstone  = byte(1)
	SeekLTAliveTombstone = byte(0)
)

// SeekLTPebbleStore implements the SeekLT (Issue-196 style) versioned store approach
type SeekLTPebbleStore struct {
	db        *pebble.DB
	batch     *pebble.Batch
	version   uint64
	keyBuffer []byte
}

// NewSeekLTPebbleStore creates the SeekLT Pebble versioned store implementation
func NewSeekLTPebbleStore(path string, version uint64) (*SeekLTPebbleStore, error) {
    // Configure options to mirror DualTimeline for best, fair read performance
    cache := pebble.NewCache(512 << 20) // 512MB block cache for reads
    opts := &pebble.Options{
        DisableWAL:               false,                                  // Enable WAL for parity
        MemTableSize:             256 << 20,                              // 256MB memtable
        MaxConcurrentCompactions: func() int { return runtime.NumCPU() }, // Match concurrency
        L0CompactionThreshold:    20,                                     // Friendlier L0 for ingest
        L0StopWritesThreshold:    40,                                     // Delay stalls during bench
        MaxOpenFiles:             5000,                                   // Reduce file-open stalls
        Cache:                    cache,                                  // Block cache
        FormatMajorVersion:       pebble.FormatNewest,
    }
    if path == "" {
        // Use in-memory FS but keep WAL enabled for parity with Sync commits in benchmarks
        opts.FS = vfs.NewMem()
    }
	
	db, err := pebble.Open(path, opts)
	if err != nil {
		return nil, err
	}

	return &SeekLTPebbleStore{
		db:        db,
		// Use IndexedBatch so iterators can see in-batch mutations (read-your-writes)
		batch:     db.NewIndexedBatch(),
		version:   version,
		keyBuffer: make([]byte, 0, 256),
	}, nil
}

// Set stores a key-value pair at the current version (SeekLT approach)
func (vs *SeekLTPebbleStore) Set(key, value []byte) error {
	k := vs.makeVersionedKey(key, vs.version)
	v := vs.valueWithTombstone(SeekLTAliveTombstone, value)
	return vs.batch.Set(k, v, nil)
}

// Delete marks a key as deleted at the current version (SeekLT approach)
func (vs *SeekLTPebbleStore) Delete(key []byte) error {
	k := vs.makeVersionedKey(key, vs.version)
	v := vs.valueWithTombstone(SeekLTDeadTombstone, nil)
	return vs.batch.Set(k, v, nil)
}

// Get retrieves the latest version of a key using the SeekLT approach
func (vs *SeekLTPebbleStore) Get(key []byte) ([]byte, error) {
	value, _, err := vs.get(key)
	return value, err
}

// get implements the SeekLT logic using SeekLT iterator positioning
func (vs *SeekLTPebbleStore) get(key []byte) (value []byte, tombstone byte, err error) {
    // Use a bounded iterator over the logical key range to minimize IO and match Canopy semantics
    // With inverted version encoding, SeekLT(key|version+1) positions at the newest entry <= version.
    var seekKey = key
    if vs.version != math.MaxUint64 {
        seekKey = vs.makeVersionedKey(key, vs.version+1)
    }

    it, err := vs.newVersionedIterator(key, true)
    if err != nil {
        return nil, 0, err
    }
    defer it.Close()

    iter := it.iter
    if !iter.SeekLT(seekKey) {
        iter.SeekGE(key)
    }
    // Find latest version ≤ version for this user key
    for ; iter.Valid(); iter.Next() {
        userKey, ver, e := vs.parseVersionedKey(iter.Key())
        if e != nil || !bytes.Equal(userKey, key) || ver > vs.version {
            continue
        }
        tombstone, value = vs.parseValueWithTombstone(iter.Value())
        return
    }
    return nil, 0, nil
}

// Iterator returns the SeekLT versioned iterator implementation
func (vs *SeekLTPebbleStore) Iterator(prefix []byte) (Iterator, error) {
	return vs.newVersionedIterator(prefix, false)
}

// newVersionedIterator creates SeekLT's versioned iterator
func (vs *SeekLTPebbleStore) newVersionedIterator(prefix []byte, reverse bool) (*SeekLTIterator, error) {
	opts := &pebble.IterOptions{LowerBound: prefix, UpperBound: prefixEnd(prefix)}
	
	var iter *pebble.Iterator
	var err error
	// Always use a DB iterator to avoid snapshot staleness in benchmarks.
	iter, err = vs.db.NewIter(opts)
	if iter == nil || err != nil {
		return nil, fmt.Errorf("failed to create iterator: %v", err)
	}
	
	return &SeekLTIterator{
		iter:    iter,
		store:   vs,
		prefix:  prefix,
		reverse: reverse,
	}, nil
}

// Commit commits the batch to the database (SeekLT approach)
func (vs *SeekLTPebbleStore) Commit() error {
	// Match benchmark-configured sync behavior for parity with other variants
	syncOpt := pebble.NoSync
	if seekltPebbleSync {
		syncOpt = pebble.Sync
	}
	if err := vs.batch.Commit(syncOpt); err != nil {
		return err
	}
	// Create a new IndexedBatch to keep read-your-writes behavior
	vs.batch = vs.db.NewIndexedBatch()
	return nil
}

// Close closes the store and releases resources
func (vs *SeekLTPebbleStore) Close() error {
	vs.batch.Close()
	return vs.db.Close()
}

// SeekLTIterator implements SeekLT iteration with single-pass key deduplication
type SeekLTIterator struct {
	iter        *pebble.Iterator
	store       *SeekLTPebbleStore
	prefix      []byte
	reverse     bool
	key         []byte
	value       []byte
	isValid     bool
	initialized bool
	lastUserKey []byte
}

// Valid returns true if the iterator is positioned at a valid entry
func (vi *SeekLTIterator) Valid() bool {
	if !vi.initialized {
		vi.first()
	}
	return vi.isValid
}

// Next advances the iterator to the next entry (SeekLT approach)
func (vi *SeekLTIterator) Next() {
	if !vi.initialized {
		vi.first()
		return
	}
	vi.advanceToNextKey()
}

// Key returns the current key (without version/tombstone suffix)
func (vi *SeekLTIterator) Key() []byte {
	if !vi.isValid {
		return nil
	}
	// Always clone for safety and to mirror Canopy iterator semantics
	return bytes.Clone(vi.key)
}

// Value returns the current value
func (vi *SeekLTIterator) Value() []byte {
	if !vi.isValid {
		return nil
	}
	// Always clone for safety and to mirror Canopy iterator semantics
	return bytes.Clone(vi.value)
}

// Close closes the iterator
func (vi *SeekLTIterator) Close() { 
	_ = vi.iter.Close() 
}

// first positions the iterator at the first valid entry (SeekLT logic)
func (vi *SeekLTIterator) first() {
	vi.initialized = true
	// Seek to proper position
	if !vi.reverse {
		if len(vi.prefix) == 0 {
			vi.iter.First()
		} else {
			vi.iter.SeekGE(vi.prefix)
		}
	} else {
		if len(vi.prefix) == 0 {
			vi.iter.Last()
		} else {
			if !vi.iter.SeekLT(prefixEnd(vi.prefix)) {
				vi.iter.Last()
			}
		}
	}
	// Go to the next 'user key'
	vi.advanceToNextKey()
}

// advanceToNextKey advances to the next unique 'user key' (SeekLT deduplication logic)
func (vi *SeekLTIterator) advanceToNextKey() {
	vi.isValid, vi.key, vi.value = false, nil, nil
	// While the iterator is valid - step to next key
	for ; vi.iter.Valid(); vi.step() {
		userKey, version, err := vi.store.parseVersionedKey(vi.iter.Key())
		if err != nil || (len(vi.prefix) > 0 && !bytes.HasPrefix(userKey, vi.prefix)) {
			continue
		}
		// Skip over the 'previous userKey' to go to the next 'userKey' (SeekLT approach)
		if version > vi.store.version || (vi.lastUserKey != nil && bytes.Equal(userKey, vi.lastUserKey)) {
			continue
		}
		// Set as 'previous userKey'
		vi.lastUserKey = bytes.Clone(userKey)
		// Now the iterator's current value is the newest visible version for userKey
		tomb, val := vi.store.parseValueWithTombstone(vi.iter.Value())
		// Skip dead user-keys
		if tomb == SeekLTDeadTombstone {
			continue
		}
		// Set variables
		vi.key, vi.value, vi.isValid = bytes.Clone(userKey), val, true
		// Exit
		return
	}
}

// step increments the iterator to the logical 'next'
func (vi *SeekLTIterator) step() {
	if vi.reverse {
		vi.iter.Prev()
	} else {
		vi.iter.Next()
	}
}

// makeVersionedKey creates SeekLT's versioned key with inverted version encoding
// k = [UserKey][InvertedVersion]
func (vs *SeekLTPebbleStore) makeVersionedKey(userKey []byte, version uint64) []byte {
	keyLength := len(userKey) + SeekLTVersionSize
	vs.keyBuffer = ensureCapacity(vs.keyBuffer, keyLength)
	// Resize buffer to actual length needed
	vs.keyBuffer = vs.keyBuffer[:keyLength]
	// Copy user key into buffer
	offset := copy(vs.keyBuffer, userKey)
	// Use the inverted version (^version) so newer versions sort first (SeekLT approach)
	binary.BigEndian.PutUint64(vs.keyBuffer[offset:], ^version)
	// Return a copy to prevent buffer reuse issues
	result := make([]byte, keyLength)
	copy(result, vs.keyBuffer)
	return result
}

// parseVersionedKey extracts components and converts back from inverted version (SeekLT approach)
// k = [UserKey][InvertedVersion]
func (vs *SeekLTPebbleStore) parseVersionedKey(versionedKey []byte) (userKey []byte, version uint64, err error) {
	if len(versionedKey) < SeekLTVersionSize {
		return nil, 0, fmt.Errorf("key too short")
	}
	// Extract user key (everything except version suffix)
	userKeyEnd := len(versionedKey) - SeekLTVersionSize
	// Extract the userKey and the version
	userKey, version = versionedKey[:userKeyEnd], binary.BigEndian.Uint64(versionedKey[userKeyEnd:])
	// Extract inverted version and convert back to real version (SeekLT approach)
	version = ^version
	return
}

// valueWithTombstone creates SeekLT's value with tombstone prefix
// v = [1-byte Tombstone][ActualValue]
func (vs *SeekLTPebbleStore) valueWithTombstone(tombstone byte, value []byte) []byte {
	v := make([]byte, 1+len(value))
	// First byte is tombstone indicator
	v[0] = tombstone
	// The rest is the value
	if len(value) > 0 {
		copy(v[1:], value)
	}
	return v
}

// parseValueWithTombstone extracts SeekLT's tombstone and actual value
// v = [1-byte Tombstone][ActualValue]
func (vs *SeekLTPebbleStore) parseValueWithTombstone(v []byte) (tombstone byte, value []byte) {
	if len(v) == 0 {
		return SeekLTDeadTombstone, nil
	}
	// Extract the value
	if len(v) > 1 {
		value = v[1:]
	}
	// First byte is tombstone indicator
	return v[0], bytes.Clone(value)
}

// prefixEnd returns the key that would sort immediately after all keys with the given prefix
func prefixEnd(prefix []byte) []byte {
    if len(prefix) == 0 {
        return nil
    }
    // Append a long run of 0xFF to guarantee the UpperBound sorts after any
    // key starting with the provided prefix, even if the next byte is 0xFF.
    const sentinelLen = 256
    out := make([]byte, len(prefix)+sentinelLen)
    copy(out, prefix)
    for i := len(prefix); i < len(out); i++ {
        out[i] = 0xFF
    }
    return out
}

// ensureCapacity ensures the buffer has at least the specified capacity
func ensureCapacity(buf []byte, capacity int) []byte {
	if cap(buf) >= capacity {
		return buf[:0]
	}
	return make([]byte, 0, capacity)
}
