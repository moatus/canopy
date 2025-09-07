package main

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
)

// SplitTimelinePebbleStore stores latest (LSS) and historical (HSS) in separate Pebble DBs
// LSS DB contains only s/<key>
// HSS DB contains h/<heightBE>/<key> (and would contain other historical families in production)
// This design isolates hot latest reads from large historical data for lower read amplification.

type SplitTimelinePebbleStore struct {
	lssDB   *pebble.DB
	hssDB   *pebble.DB
	version uint64
	lssBatch *pebble.Batch
	hssBatch *pebble.Batch
}

// splitPebbleSync controls fsync behavior for SplitTimeline commits.
// Default true to mirror production parity; benchmarks may choose durability trade-offs separately.
var splitPebbleSync = true

// Tunable knobs for Split Timeline (env-overridable):
// - LSS gets more resources by default to favor latest reads.
var (
	// Cache sizes in MB
	splitLSSCacheMB = 1024
	splitHSSCacheMB = 256
	// Max concurrent compactions per DB
	splitLSSCompactions = func() int { return runtime.NumCPU() }()
	splitHSSCompactions = 1
)

func init() {
	// Helper to parse positive ints from env
	parseInt := func(s string, def int) int {
		s = strings.TrimSpace(s)
		if s == "" { return def }
		if n, err := strconv.Atoi(s); err == nil && n > 0 { return n }
		return def
	}
	if v := os.Getenv("LSS_CACHE_MB"); v != "" {
		splitLSSCacheMB = parseInt(v, splitLSSCacheMB)
	}
	if v := os.Getenv("HSS_CACHE_MB"); v != "" {
		splitHSSCacheMB = parseInt(v, splitHSSCacheMB)
	}
	if v := os.Getenv("LSS_MAX_COMPACTIONS"); v != "" {
		splitLSSCompactions = parseInt(v, splitLSSCompactions)
	}
	if v := os.Getenv("HSS_MAX_COMPACTIONS"); v != "" {
		splitHSSCompactions = parseInt(v, splitHSSCompactions)
	}
}

// NewSplitTimelinePebbleStore opens two Pebble DBs (or in-memory FS when paths are empty).
// Keep options similar to DualTimeline for production-like parity.
func NewSplitTimelinePebbleStore(lssPath, hssPath string, version uint64) (*SplitTimelinePebbleStore, error) {
	// Configure LSS DB (optimize for low-latency point reads / narrow scans)
	lssCache := pebble.NewCache(int64(splitLSSCacheMB) << 20)
	lssOpts := &pebble.Options{
		DisableWAL:               false,
		MemTableSize:             256 << 20,
		MaxConcurrentCompactions: func() int { return splitLSSCompactions },
		L0CompactionThreshold:    20,
		L0StopWritesThreshold:    40,
		MaxOpenFiles:             5000,
		Cache:                    lssCache,
		FormatMajorVersion:       pebble.FormatNewest,
	}
	if lssPath == "" {
		lssOpts.FS = vfs.NewMem()
	}

	// Configure HSS DB (optimize for range scans and bulk writes)
	hssCache := pebble.NewCache(int64(splitHSSCacheMB) << 20)
	hssOpts := &pebble.Options{
		DisableWAL:               false,
		MemTableSize:             256 << 20,
		MaxConcurrentCompactions: func() int { return splitHSSCompactions },
		L0CompactionThreshold:    20,
		L0StopWritesThreshold:    40,
		MaxOpenFiles:             5000,
		Cache:                    hssCache,
		FormatMajorVersion:       pebble.FormatNewest,
	}
	if hssPath == "" {
		hssOpts.FS = vfs.NewMem()
	}

	lssDB, err := pebble.Open(lssPath, lssOpts)
	if err != nil {
		return nil, fmt.Errorf("open LSS DB: %w", err)
	}
	hssDB, err := pebble.Open(hssPath, hssOpts)
	if err != nil {
		_ = lssDB.Close()
		return nil, fmt.Errorf("open HSS DB: %w", err)
	}

	return &SplitTimelinePebbleStore{
		lssDB:    lssDB,
		hssDB:    hssDB,
		version:  version,
		lssBatch: lssDB.NewBatch(),
		hssBatch: hssDB.NewBatch(),
	}, nil
}

// Set writes to both latest and historical stores in one logical batch (commit later).
func (s *SplitTimelinePebbleStore) Set(key, value []byte) error {
	if err := s.lssBatch.Set(keyLSS(key), value, nil); err != nil {
		return err
	}
	return s.hssBatch.Set(keyHSS(s.version, key), value, nil)
}

// Get returns value from latest store (s/<key>).
func (s *SplitTimelinePebbleStore) Get(key []byte) ([]byte, error) {
	val, closer, err := s.lssDB.Get(keyLSS(key))
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	out := make([]byte, len(val))
	copy(out, val)
	return out, nil
}

// Iterator returns an iterator over latest store (s/), bounded by ["s/","t/").
func (s *SplitTimelinePebbleStore) Iterator(prefix []byte) (Iterator, error) {
	var opts *pebble.IterOptions
	if len(prefix) == 0 {
		opts = &pebble.IterOptions{LowerBound: []byte("s/"), UpperBound: []byte("t/")}
	} else {
		lssPrefix := keyLSS(prefix)
		opts = &pebble.IterOptions{LowerBound: lssPrefix, UpperBound: append(lssPrefix, 0xff)}
	}
	it, err := s.lssDB.NewIter(opts)
	if err != nil {
		return nil, err
	}
	it.First()
	return &lssSplitIterator{it: it, prefixLen: len("s/")}, nil
}

// HistoricalIterator iterates over the historical store for a given version (height).
func (s *SplitTimelinePebbleStore) HistoricalIterator(version uint64, prefix []byte) (Iterator, error) {
	lb, ub := boundsForHeight(version)
	// If a user prefix is provided, tighten bounds to h/<version>/<prefix>
	if len(prefix) > 0 {
		base := keyHSS(version, prefix)
		lb, ub = base, append(base, 0xff)
	}
	it, err := s.hssDB.NewIter(&pebble.IterOptions{LowerBound: lb, UpperBound: ub})
	if err != nil {
		return nil, err
	}
	it.First()
	return &splitPebbleIterator{it: it, stripLen: 2 + 8 + 1}, nil // strip "h/" + 8B + "/"
}

// Commit flushes both batches. HSS first, then LSS, to match a production-safe ordering.
func (s *SplitTimelinePebbleStore) Commit() error {
	syncOpt := pebble.NoSync
	if splitPebbleSync {
		syncOpt = pebble.Sync
	}
	if err := s.hssBatch.Commit(syncOpt); err != nil {
		return err
	}
	if err := s.lssBatch.Commit(syncOpt); err != nil {
		return err
	}
	// create new batches for subsequent operations
	s.hssBatch = s.hssDB.NewBatch()
	s.lssBatch = s.lssDB.NewBatch()
	return nil
}

// Close releases resources.
func (s *SplitTimelinePebbleStore) Close() error {
	if s.hssBatch != nil {
		_ = s.hssBatch.Close()
	}
	if s.lssBatch != nil {
		_ = s.lssBatch.Close()
	}
	if err := s.hssDB.Close(); err != nil {
		_ = s.lssDB.Close()
		return err
	}
	return s.lssDB.Close()
}

// splitPebbleIterator is a simple iterator wrapper that strips a fixed prefix length.
type splitPebbleIterator struct {
	it       *pebble.Iterator
	stripLen int
}

func (pi *splitPebbleIterator) Valid() bool { return pi.it.Valid() }
func (pi *splitPebbleIterator) Next()       { pi.it.Next() }
func (pi *splitPebbleIterator) Key() []byte {
	k := pi.it.Key()
	if len(k) > pi.stripLen {
		out := make([]byte, len(k)-pi.stripLen)
		copy(out, k[pi.stripLen:])
		return out
	}
	out := make([]byte, len(k))
	copy(out, k)
	return out
}
func (pi *splitPebbleIterator) Value() []byte {
	v := pi.it.Value()
	out := make([]byte, len(v))
	copy(out, v)
	return out
}
func (pi *splitPebbleIterator) Close() { pi.it.Close() }

// lssSplitIterator wraps an iterator over s/ and strips the s/ prefix, always copying outputs.
type lssSplitIterator struct {
	it        *pebble.Iterator
	prefixLen int
}

func (pi *lssSplitIterator) Valid() bool { return pi.it.Valid() }
func (pi *lssSplitIterator) Next()       { pi.it.Next() }
func (pi *lssSplitIterator) Key() []byte {
	key := pi.it.Key()
	if len(key) > pi.prefixLen {
		k := make([]byte, len(key)-pi.prefixLen)
		copy(k, key[pi.prefixLen:])
		return k
	}
	k := make([]byte, len(key))
	copy(k, key)
	return k
}
func (pi *lssSplitIterator) Value() []byte {
	v := pi.it.Value()
	out := make([]byte, len(v))
	copy(out, v)
	return out
}
func (pi *lssSplitIterator) Close() { pi.it.Close() }
