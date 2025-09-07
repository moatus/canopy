package main

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/alecthomas/units"
	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"
	"github.com/stretchr/testify/require"
)

// System-level benchmark comparing Badger (current system), Dual Timeline Pebble, and Pebble Issue-196
// This maintains the system test approach by using versioned store abstractions

var (
	numKeys     = 10_000 // Default to 10k for practical runtime by default
	numVersions = 50     // Default to 50 versions to emulate Canopy block history
)

const lssVersion = math.MaxUint64

// doCopy toggles whether Pebble iterators copy returned key/value slices.
// Badger always copies values by API design in this harness.
var doCopy = true

// versionsList holds scenarios to run within a single benchmark (e.g., 2,4,8,16)
var versionsList []int

// badgerPrefetch toggles whether Badger Iterator prefetches values
// Production canopy iterators do NOT set PrefetchValues (zero-value false),
// so default this to false for production-equivalent behavior.
var badgerPrefetch = false

// pebbleSync controls whether Pebble batches fsync on commit (WAL durability).
// In production, Badger uses SyncWrites=true; set default to true for parity.
var pebbleSync = true

// valueBytes controls the size of the values written for all stores in system benchmarks.
// Default small (32 bytes). Increase (e.g., 4096) to exercise Badger's vlog path.
var valueBytes = 32

// keyShape controls how keys are generated: "flat" ("0","1",...) or "realistic"
// which mimics Canopy key families (account_, validator_, committee_, hash_).
var keyShape = "realistic"

// prefixList allows narrowing prefix scans. Comma-separated values like
// "account_,committee_". If empty and keyShape=="realistic", defaults to
// [account_, validator_, committee_].
var prefixList []string

// badgerValueThreshold controls Badger's value placement (inline vs vlog).
// Default 1024 as in production; can be overridden to test sensitivity.
var badgerValueThreshold = 1024

// badgerSyncWrites mirrors Badger's SyncWrites option; default true for production parity.
var badgerSyncWrites = true

func init() {
	if v := strings.TrimSpace(os.Getenv("NUM_KEYS")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			numKeys = n
		}
	}
	if v := strings.TrimSpace(os.Getenv("NUM_VERSIONS")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			numVersions = n
		}
	}
	if v := strings.ToLower(strings.TrimSpace(os.Getenv("COPY_MODE"))); v != "" {
		// Accept values: "copy", "nocopy", "1", "0", "true", "false"
		switch v {
		case "copy", "1", "true", "yes", "y":
			doCopy = true
		case "nocopy", "0", "false", "no", "n":
			doCopy = false
		}
	}
	if v := strings.TrimSpace(os.Getenv("NUM_VERSIONS_LIST")); v != "" {
		parts := strings.Split(v, ",")
		for _, p := range parts {
			p = strings.TrimSpace(p)
			if n, err := strconv.Atoi(p); err == nil && n > 0 {
				versionsList = append(versionsList, n)
			}
		}
	}
	if len(versionsList) == 0 {
		versionsList = []int{numVersions}
	}
	if v := strings.ToLower(strings.TrimSpace(os.Getenv("BADGER_PREFETCH"))); v != "" {
		switch v {
		case "0", "false", "no", "n":
			badgerPrefetch = false
		default:
			badgerPrefetch = true
		}
	}
	if v := strings.ToLower(strings.TrimSpace(os.Getenv("PEBBLE_SYNC"))); v != "" {
		switch v {
		case "0", "false", "no", "n":
			pebbleSync = false
		default:
			pebbleSync = true
		}
	}
	seekltPebbleSync = pebbleSync
	splitPebbleSync = pebbleSync
	if v := strings.TrimSpace(os.Getenv("BADGER_VALUE_THRESHOLD")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			badgerValueThreshold = n
		}
	}
	if v := strings.ToLower(strings.TrimSpace(os.Getenv("BADGER_SYNC_WRITES"))); v != "" {
		switch v {
		case "0", "false", "no", "n":
			badgerSyncWrites = false
		default:
			badgerSyncWrites = true
		}
	}
	if v := strings.TrimSpace(os.Getenv("VALUE_BYTES")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			valueBytes = n
		}
	}
	if v := strings.TrimSpace(os.Getenv("KEY_SHAPE")); v != "" {
		vv := strings.ToLower(v)
		if vv == "realistic" || vv == "flat" {
			keyShape = vv
		}
	}
	if v := strings.TrimSpace(os.Getenv("PREFIX_LIST")); v != "" {
		parts := strings.Split(v, ",")
		for _, p := range parts {
			p = strings.TrimSpace(p)
			if p != "" {
				prefixList = append(prefixList, p)
			}
		}
	}
}

// progressEnabled controls optional console progress logs for heavy pre-population phases.
// Enable by running with environment variable PROGRESS=1 (or true/yes).
func progressEnabled() bool {
    v := strings.ToLower(strings.TrimSpace(os.Getenv("PROGRESS")))
    if v == "" || v == "0" || v == "false" || v == "no" || v == "n" {
        return false
    }
    return true
}

// quiesceEnabled controls whether we precondition the LSS range (s/..t/) before timing
// latest scans by flushing and compacting that range. Enable with QUIESCE=1.
// This is a benchmark-only option to emulate steady-state read conditions.
func quiesceEnabled() bool {
    v := strings.ToLower(strings.TrimSpace(os.Getenv("QUIESCE")))
    // Default to true to represent steady-state best performance for all variants.
    if v == "0" || v == "false" || v == "no" || v == "n" {
        return false
    }
    return true
}

// Interfaces moved to versioned_store_interfaces.go for reuse by non-test stores

// BadgerVersionedStore implements VersionedStore using Badger with LSS/HSS pattern
type BadgerVersionedStore struct {
	db       *badger.DB
	version  uint64
	lssBatch *badger.WriteBatch
	hssBatch *badger.WriteBatch
}

// NewBadgerVersionedStore creates a new Badger-based versioned store
func NewBadgerVersionedStore(path string, version uint64) (*BadgerVersionedStore, error) {
	opts := badger.DefaultOptions(path).
		WithNumVersionsToKeep(math.MaxInt64).
		WithLoggingLevel(badger.ERROR).
		WithMemTableSize(int64(256 * units.MB)).
		WithValueThreshold(int64(badgerValueThreshold)).
		WithCompression(options.None).
		WithNumMemtables(16).
		WithNumLevelZeroTables(10).
		WithNumLevelZeroTablesStall(20).
		WithBaseTableSize(128 << 20).
		WithBaseLevelSize(512 << 20).
		WithCompactL0OnClose(true).
		WithNumCompactors(runtime.NumCPU()).
		WithBypassLockGuard(true).
		WithDetectConflicts(false).
		WithSyncWrites(badgerSyncWrites) // Production-equivalent default

	if path == "" {
		opts = opts.WithInMemory(true)
	}

	db, err := badger.OpenManaged(opts)
	if err != nil {
		return nil, err
	}

	return &BadgerVersionedStore{
		db:       db,
		version:  version,
		lssBatch: db.NewWriteBatchAt(lssVersion),
		hssBatch: db.NewWriteBatchAt(version),
	}, nil
}

func (b *BadgerVersionedStore) Set(key, value []byte) error {
	// Write to both LSS (latest) and HSS (historical) like the actual system
	lssKey := append([]byte("s/"), key...)
	hssKey := append([]byte("h/"), key...)

	if err := b.lssBatch.Set(lssKey, value); err != nil {
		return err
	}
	return b.hssBatch.Set(hssKey, value)
}

func (b *BadgerVersionedStore) Get(key []byte) ([]byte, error) {
	// Read from LSS for latest version
	lssKey := append([]byte("s/"), key...)
	tx := b.db.NewTransactionAt(math.MaxUint64, false)
	defer tx.Discard()

	item, err := tx.Get(lssKey)
	if err != nil {
		return nil, err
	}

	return item.ValueCopy(nil)
}

func (b *BadgerVersionedStore) Iterator(prefix []byte) (Iterator, error) {
	tx := b.db.NewTransactionAt(math.MaxUint64, false)
	var opts badger.IteratorOptions
	if len(prefix) == 0 {
		// For nil prefix, iterate over all LSS keys
		opts = badger.IteratorOptions{Prefix: []byte("s/"), PrefetchValues: badgerPrefetch}
	} else {
		lssPrefix := append([]byte("s/"), prefix...)
		opts = badger.IteratorOptions{Prefix: lssPrefix, PrefetchValues: badgerPrefetch}
	}
	it := tx.NewIterator(opts)
	it.Rewind() // Start iteration
	return &BadgerIterator{it: it, tx: tx, prefixLen: len("s/")}, nil
}

// HistoricalIterator iterates over the historical (HSS) keyspace at a specific version
func (b *BadgerVersionedStore) HistoricalIterator(version uint64, prefix []byte) (Iterator, error) {
	tx := b.db.NewTransactionAt(version, false)
	var opts badger.IteratorOptions
	if len(prefix) == 0 {
		opts = badger.IteratorOptions{Prefix: []byte("h/"), PrefetchValues: badgerPrefetch}
	} else {
		hssPrefix := append([]byte("h/"), prefix...)
		opts = badger.IteratorOptions{Prefix: hssPrefix, PrefetchValues: badgerPrefetch}
	}
	it := tx.NewIterator(opts)
	it.Rewind()
	return &BadgerIterator{it: it, tx: tx, prefixLen: len("h/")}, nil
}

func (b *BadgerVersionedStore) Commit() error {
	if err := b.lssBatch.Flush(); err != nil {
		return err
	}
	if err := b.hssBatch.Flush(); err != nil {
		return err
	}
	// Create new batches for next operations
	b.lssBatch = b.db.NewWriteBatchAt(lssVersion)
	b.hssBatch = b.db.NewWriteBatchAt(b.version)
	return nil
}

func (b *BadgerVersionedStore) Close() error {
	if b.lssBatch != nil {
		b.lssBatch.Cancel()
	}
	if b.hssBatch != nil {
		b.hssBatch.Cancel()
	}
	return b.db.Close()
}

// BadgerIterator wraps badger iterator
type BadgerIterator struct {
	it        *badger.Iterator
	tx        *badger.Txn
	prefixLen int
}

func (bi *BadgerIterator) Valid() bool { return bi.it.Valid() }
func (bi *BadgerIterator) Next()       { bi.it.Next() }
func (bi *BadgerIterator) Key() []byte {
	// Strip the "s/" prefix to return original key, and copy to normalize with Badger store
	key := bi.it.Item().Key()
	if len(key) > bi.prefixLen {
		k := make([]byte, len(key)-bi.prefixLen)
		copy(k, key[bi.prefixLen:])
		return k
	}
	k := make([]byte, len(key))
	copy(k, key)
	return k
}

func (bi *BadgerIterator) Value() []byte {
	val, _ := bi.it.Item().ValueCopy(nil)
	return val
}

func (bi *BadgerIterator) Close() {
	bi.it.Close()
	bi.tx.Discard()
}

// DualTimelinePebbleStore implements VersionedStore using Pebble with Dual Timeline (LSS/HSS)
type DualTimelinePebbleStore struct {
	db      *pebble.DB
	version uint64
	batch   *pebble.Batch
}

func NewDualTimelinePebbleStore(path string, version uint64) (*DualTimelinePebbleStore, error) {
    cache := pebble.NewCache(512 << 20) // 512MB block cache improves read hit ratio
    opts := &pebble.Options{
        DisableWAL:               false,                                  // Enable WAL for fair comparison with Badger's sync writes
        MemTableSize:             256 << 20,                              // 256MB to match Badger
        MaxConcurrentCompactions: func() int { return runtime.NumCPU() }, // Match Badger's compactor count
        L0CompactionThreshold:    20,                                     // Friendlier L0 for bulk ingest
        L0StopWritesThreshold:    40,                                     // Delay stalls during benchmarking
        MaxOpenFiles:             5000,                                   // Fewer iterator file-open stalls
        Cache:                    cache,                                   // Block cache for reads
        FormatMajorVersion:       pebble.FormatNewest,
    }

	if path == "" {
		opts.FS = vfs.NewMem()
	}

	db, err := pebble.Open(path, opts)
	if err != nil {
		return nil, err
	}

	return &DualTimelinePebbleStore{
		db:      db,
		version: version,
		batch:   db.NewBatch(),
	}, nil
}

func (p *DualTimelinePebbleStore) Set(key, value []byte) error {
	// Use the same LSS/HSS pattern as the helper functions
	lssKey := keyLSS(key)
	hssKey := keyHSS(p.version, key)

	if err := p.batch.Set(lssKey, value, nil); err != nil {
		return err
	}
	return p.batch.Set(hssKey, value, nil)
}

func (p *DualTimelinePebbleStore) Get(key []byte) ([]byte, error) {
	lssKey := keyLSS(key)
	val, closer, err := p.db.Get(lssKey)
	if err != nil {
		return nil, err
	}
	defer closer.Close()

	// Copy the value since closer will be called
	result := make([]byte, len(val))
	copy(result, val)
	return result, nil
}

func (p *DualTimelinePebbleStore) Iterator(prefix []byte) (Iterator, error) {
	// For empty prefix, iterate over all LSS keys
	var opts *pebble.IterOptions
	if len(prefix) == 0 {
		opts = &pebble.IterOptions{
			LowerBound: []byte("s/"),
			UpperBound: []byte("t/"), // Next prefix after "s/"
		}
	} else {
		lssPrefix := keyLSS(prefix)
		opts = &pebble.IterOptions{
			LowerBound: lssPrefix,
			UpperBound: append(lssPrefix, 0xff),
		}
	}

	it, err := p.db.NewIter(opts)
	if err != nil {
		return nil, err
	}
	it.First() // Start iteration
	return &PebbleIterator{it: it, prefixLen: len("s/")}, nil
}

func (p *DualTimelinePebbleStore) Commit() error {
	syncOpt := pebble.NoSync
	if pebbleSync {
		syncOpt = pebble.Sync
	}
	err := p.batch.Commit(syncOpt)
	if err != nil {
		return err
	}
	// Create new batch for next operations
	p.batch = p.db.NewBatch()
	return nil
}

func (p *DualTimelinePebbleStore) Close() error {
	p.batch.Close()
	return p.db.Close()
}

// PebbleIterator wraps pebble iterator for Dual Timeline Pebble
type PebbleIterator struct {
	it        *pebble.Iterator
	prefixLen int
}

func (pi *PebbleIterator) Valid() bool { return pi.it.Valid() }
func (pi *PebbleIterator) Next()       { pi.it.Next() }
func (pi *PebbleIterator) Key() []byte {
	// Strip the "s/" prefix; optionally copy to match Badger semantics
	key := pi.it.Key()
	if len(key) > pi.prefixLen {
		if doCopy {
			k := make([]byte, len(key)-pi.prefixLen)
			copy(k, key[pi.prefixLen:])
			return k
		}
		return key[pi.prefixLen:]
	}
	if doCopy {
		k := make([]byte, len(key))
		copy(k, key)
		return k
	}
	return key
}

func (pi *PebbleIterator) Value() []byte {
	v := pi.it.Value()
	if doCopy {
		out := make([]byte, len(v))
		copy(out, v)
		return out
	}
	return v
}

func (pi *PebbleIterator) Close() { pi.it.Close() }

// Issue196Iterator implements the expensive SeekLT pattern that Issue-196 requires
type Issue196Iterator struct {
	db      *pebble.DB
	prefix  []byte
	keys    [][]byte // All logical keys to iterate through
	current int      // Current position in keys slice
	done    bool
	it      *pebble.Iterator // Reused iterator for SeekLT lookups
}

func (i *Issue196Iterator) Valid() bool {
	return !i.done && i.current >= 0 && i.current < len(i.keys)
}

func (i *Issue196Iterator) Next() {
	i.current++
	if i.current >= len(i.keys) {
		i.done = true
	}
}

func (i *Issue196Iterator) Key() []byte {
	if i.current < len(i.keys) {
		return i.keys[i.current]
	}
	return nil
}

func (i *Issue196Iterator) Value() []byte {
	if i.current >= len(i.keys) {
		return nil
	}
	// Reuse a single iterator for SeekLT; optionally copy value
	vkey := versionedKey(i.keys[i.current], math.MaxUint64, false)
	if i.it.SeekLT(vkey) && i.it.Valid() {
		v := i.it.Value()
		if doCopy {
			out := make([]byte, len(v))
			copy(out, v)
			return out
		}
		return v
	}
	return nil
}

func (i *Issue196Iterator) Close() {
	i.done = true
	if i.it != nil {
		i.it.Close()
	}
}

// BenchmarkResult holds the results of a single benchmark run
type BenchmarkResult struct {
	Name        string
	NsPerOp     int64
	AllocsPerOp int64
	BytesPerOp  int64
}

var benchResults []BenchmarkResult

func beginBenchCollection() { benchResults = nil }

func collectResult(name string, iterations int, elapsed time.Duration, before, after runtime.MemStats) {
	if iterations <= 0 {
		iterations = 1
	}
	ns := elapsed.Nanoseconds() / int64(iterations)
	allocs := int64(0)
	bytes := int64(0)
	if after.Mallocs >= before.Mallocs {
		allocs = int64(after.Mallocs-before.Mallocs) / int64(iterations)
	}
	if after.TotalAlloc >= before.TotalAlloc {
		bytes = int64(after.TotalAlloc-before.TotalAlloc) / int64(iterations)
	}
	benchResults = append(benchResults, BenchmarkResult{
		Name:        name,
		NsPerOp:     ns,
		AllocsPerOp: allocs,
		BytesPerOp:  bytes,
	})
}

// formatCount formats plain counts (e.g., allocations) with thousands separators
func formatCount(n int64) string {
	// Handle negative just in case
	sign := ""
	if n < 0 {
		sign = "-"
		n = -n
	}
	s := strconv.FormatInt(n, 10)
	// Insert commas
	if len(s) <= 3 {
		return sign + s
	}
	var b strings.Builder
	b.Grow(len(s) + len(s)/3)
	rem := len(s) % 3
	if rem == 0 {
		rem = 3
	}
	b.WriteString(s[:rem])
	for i := rem; i < len(s); i += 3 {
		b.WriteByte(',')
		b.WriteString(s[i : i+3])
	}
	return sign + b.String()
}

// formatDuration converts nanoseconds to a human-readable duration string
func formatDuration(ns int64) string {
	// Prefer seconds, then milliseconds, then nanoseconds for readability
	if ns >= int64(time.Second) {
		return fmt.Sprintf("%.3fs", float64(ns)/float64(time.Second))
	}
	if ns >= int64(time.Millisecond) {
		return fmt.Sprintf("%.3fms", float64(ns)/float64(time.Millisecond))
	}
	return fmt.Sprintf("%dns", ns)
}

// formatBytes converts bytes to a human-readable string
func formatBytes(bytes int64) string {
	if bytes == 0 {
		return "0B"
	}
	switch {
	case bytes < 1024:
		return fmt.Sprintf("%dB", bytes)
	case bytes < 1024*1024:
		return fmt.Sprintf("%.1fKB", float64(bytes)/1024)
	case bytes < 1024*1024*1024:
		return fmt.Sprintf("%.1fMB", float64(bytes)/(1024*1024))
	default:
		return fmt.Sprintf("%.1fGB", float64(bytes)/(1024*1024*1024))
	}
}

// Explicit unit formatting helpers for group-consistent display
type timeDispUnit int

const (
	timeUnitNS timeDispUnit = iota
	timeUnitMS
	timeUnitS
)

func chooseTimeUnit(maxNs int64) timeDispUnit {
	switch {
	case maxNs >= int64(time.Second):
		return timeUnitS
	case maxNs >= int64(time.Millisecond):
		return timeUnitMS
	default:
		return timeUnitNS
	}
}

func timeUnitLabel(u timeDispUnit) string {
	switch u {
	case timeUnitS:
		return "s"
	case timeUnitMS:
		return "ms"
	default:
		return "ns"
	}
}

func formatDurationWithUnit(ns int64, u timeDispUnit) string {
	switch u {
	case timeUnitS:
		return fmt.Sprintf("%.3fs", float64(ns)/float64(time.Second))
	case timeUnitMS:
		return fmt.Sprintf("%.3fms", float64(ns)/float64(time.Millisecond))
	default:
		return fmt.Sprintf("%dns", ns)
	}
}

type byteDispUnit int

const (
	byteUnitB byteDispUnit = iota
	byteUnitKB
	byteUnitMB
	byteUnitGB
)

func chooseByteUnit(maxBytes int64) byteDispUnit {
	switch {
	case maxBytes >= 1024*1024*1024:
		return byteUnitGB
	case maxBytes >= 1024*1024:
		return byteUnitMB
	case maxBytes >= 1024:
		return byteUnitKB
	default:
		return byteUnitB
	}
}

func byteUnitLabel(u byteDispUnit) string {
	switch u {
	case byteUnitGB:
		return "GB"
	case byteUnitMB:
		return "MB"
	case byteUnitKB:
		return "KB"
	default:
		return "B"
	}
}

func formatBytesWithUnit(b int64, u byteDispUnit) string {
	switch u {
	case byteUnitGB:
		return fmt.Sprintf("%.1fGB", float64(b)/(1024*1024*1024))
	case byteUnitMB:
		return fmt.Sprintf("%.1fMB", float64(b)/(1024*1024))
	case byteUnitKB:
		return fmt.Sprintf("%.1fKB", float64(b)/1024)
	default:
		return fmt.Sprintf("%dB", b)
	}
}

// printBenchmarkTable displays results in a formatted table
func printBenchmarkTable(results []BenchmarkResult, title string) {
	if len(results) == 0 {
		return
	}

	fmt.Printf("\n%s\n", title)
	fmt.Printf("%s\n", strings.Repeat("=", len(title)))

	// Define stable variant order
	variantOrder := []string{"Badger", "DualTimelinePebble", "SplitTimelinePebble", "SeekLTPebble"}
	orderIndex := map[string]int{"Badger": 0, "DualTimelinePebble": 1, "SplitTimelinePebble": 2, "SeekLTPebble": 3}

	// Group results by logical test name (suffix after first '-')
	type triple struct{ Time, Allocs, Bytes string }
	groups := make(map[string]map[string]BenchmarkResult)
	for _, r := range results {
		variant := r.Name
		group := ""
		if idx := strings.Index(r.Name, "-"); idx >= 0 {
			variant = r.Name[:idx]
			group = r.Name[idx+1:]
			// Normalize Badger's naming that uses a "System-" prefix so it
			// groups with equivalent operations from other variants.
			if strings.HasPrefix(group, "System-") {
				group = strings.TrimPrefix(group, "System-")
			}
			// Lump methods under the same logical operation: remove method suffixes
			if strings.HasSuffix(group, "-Iterator") {
				group = strings.TrimSuffix(group, "-Iterator")
			}
			if strings.HasSuffix(group, "-SeekLT") {
				group = strings.TrimSuffix(group, "-SeekLT")
			}
		} else {
			// Names without '-' (e.g., simple write perf) -> use title as group
			group = title
		}
		if groups[group] == nil {
			groups[group] = make(map[string]BenchmarkResult)
		}
		groups[group][variant] = r
	}

	// Sort group names for deterministic output
	groupNames := make([]string, 0, len(groups))
	for g := range groups {
		groupNames = append(groupNames, g)
	}
	sort.Strings(groupNames)

	// Determine column width for variant name
	maxNameLen := 12
	for _, name := range variantOrder {
		if len(name) > maxNameLen {
			maxNameLen = len(name)
		}
	}
	if maxNameLen < 18 { // keep a reasonable width
		maxNameLen = 18
	}

	for gi, g := range groupNames {
		if gi > 0 {
			fmt.Println()
		}
		// Print a sub-header per group
		fmt.Printf("%s\n", g)
		fmt.Printf("%s\n", strings.Repeat("-", len(g)))
		// Determine group-wide units based on maxima
		var maxNs int64
		var maxBytes int64
		for _, r := range groups[g] {
			if r.NsPerOp > maxNs {
				maxNs = r.NsPerOp
			}
			if r.BytesPerOp > maxBytes {
				maxBytes = r.BytesPerOp
			}
		}
		tUnit := chooseTimeUnit(maxNs)
		bUnit := chooseByteUnit(maxBytes)
		timeHeader := fmt.Sprintf("Time/Op (%s)", timeUnitLabel(tUnit))
		bytesHeader := fmt.Sprintf("Bytes/Op (%s)", byteUnitLabel(bUnit))
		// Determine dynamic column widths for pretty alignment
		timeCol := len(timeHeader)
		if timeCol < 12 {
			timeCol = 12
		}
		bytesCol := len(bytesHeader)
		if bytesCol < 12 {
			bytesCol = 12
		}
		headerFormat := fmt.Sprintf("%%-%ds | %%%ds | %%12s | %%%ds\n", maxNameLen, timeCol, bytesCol)
		rowFormat := fmt.Sprintf("%%-%ds | %%%ds | %%12s | %%%ds\n", maxNameLen, timeCol, bytesCol)
		fmt.Printf(headerFormat, "Variant", timeHeader, "Allocs/Op", bytesHeader)
		sepLen := maxNameLen + timeCol + 12 + bytesCol + 9
		fmt.Printf("%s\n", strings.Repeat("-", sepLen))

		// Print rows in stable variant order, followed by any others
		printed := make(map[string]bool)
		for _, v := range variantOrder {
			if r, ok := groups[g][v]; ok {
				fmt.Printf(rowFormat, v, formatDurationWithUnit(r.NsPerOp, tUnit), formatCount(r.AllocsPerOp), formatBytesWithUnit(r.BytesPerOp, bUnit))
				printed[v] = true
			}
		}
		// If there are any additional variants, print them after in name order
		extra := make([]string, 0)
		for v := range groups[g] {
			if !printed[v] {
				extra = append(extra, v)
			}
		}
		sort.Slice(extra, func(i, j int) bool {
			// Keep a consistent relative ordering: known variants first by orderIndex, then by name
			oi, oki := orderIndex[extra[i]]
			oj, okj := orderIndex[extra[j]]
			if oki && okj {
				return oi < oj
			}
			if oki != okj {
				return oki // known first
			}
			return extra[i] < extra[j]
		})
		for _, v := range extra {
			r := groups[g][v]
			fmt.Printf(rowFormat, v, formatDurationWithUnit(r.NsPerOp, tUnit), formatCount(r.AllocsPerOp), formatBytesWithUnit(r.BytesPerOp, bUnit))
		}
	}
	fmt.Println()
}

// Benchmark_IterationStressTest tests iteration performance with large datasets across many versions
func Benchmark_IterationStressTest(b *testing.B) {
	beginBenchCollection()
	// Stress test parameters - configurable via environment
	stressKeys := 100_000 // 100K keys per version
	stressVersions := 50  // 50 versions (much higher than current 2)

	if v := strings.TrimSpace(os.Getenv("STRESS_KEYS")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			stressKeys = n
		}
	}
	if v := strings.TrimSpace(os.Getenv("STRESS_VERSIONS")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			stressVersions = n
		}
	}

	b.Logf("Iteration Stress Test: %d keys across %d versions", stressKeys, stressVersions)

	// Generate realistic keys based on actual Canopy usage patterns
	keys := make([][]byte, stressKeys)
	for i := 0; i < stressKeys; i++ {
		// Mix of different key types found in Canopy:
		switch i % 4 {
		case 0: // Account keys (20-byte addresses)
			addr := make([]byte, 20)
			binary.BigEndian.PutUint64(addr[12:], uint64(i)) // Put counter in last 8 bytes
			keys[i] = append([]byte("account_"), addr...)
		case 1: // Validator keys (20-byte addresses)
			addr := make([]byte, 20)
			binary.BigEndian.PutUint64(addr[12:], uint64(i))
			keys[i] = append([]byte("validator_"), addr...)
		case 2: // Committee keys (chainId + stake + address)
			addr := make([]byte, 20)
			binary.BigEndian.PutUint64(addr[12:], uint64(i))
			chainId := make([]byte, 8)
			binary.BigEndian.PutUint64(chainId, uint64(i%10)) // 10 different chains
			stake := make([]byte, 8)
			binary.BigEndian.PutUint64(stake, uint64(1000+i))
			keys[i] = append(append(append([]byte("committee_"), chainId...), stake...), addr...)
		case 3: // Hash-based keys (32-byte hashes)
			hash := sha256.Sum256([]byte(fmt.Sprintf("hash_input_%d", i)))
			keys[i] = append([]byte("hash_"), hash[:]...)
		}
	}

	b.Run("Badger-Stress-Iterator", func(b *testing.B) {
		b.ReportAllocs()

		// Pre-populate with many versions
		store, err := NewBadgerVersionedStore("", 1)
		require.NoError(b, err)
		defer store.Close()

		b.Logf("Populating %d versions with %d keys each...", stressVersions, stressKeys)
		for v := 1; v <= stressVersions; v++ {
			store.version = uint64(v)
			store.hssBatch = store.db.NewWriteBatchAt(uint64(v))
			store.lssBatch = store.db.NewWriteBatchAt(lssVersion)

			for j := 0; j < stressKeys; j++ {
				require.NoError(b, store.Set(keys[j], []byte(fmt.Sprintf("value_v%d_%d", v, j))))
			}
			require.NoError(b, store.Commit())
            if progressEnabled() {
                fmt.Printf("[Progress] Badger populate v %d/%d (%d keys)\n", v, stressVersions, stressKeys)
            }
		}

		b.ResetTimer()
		var ms1, ms2 runtime.MemStats
		runtime.ReadMemStats(&ms1)
		t0 := time.Now()
		for i := 0; i < b.N; i++ {
			it, err := store.Iterator(nil)
			require.NoError(b, err)

			count := 0
			for it.Valid() {
				_ = it.Key()
				_ = it.Value()
				it.Next()
				count++
			}
			it.Close()

			if count != stressKeys {
				b.Fatalf("Expected %d keys, got %d", stressKeys, count)
			}
		}
		dur := time.Since(t0)
		runtime.ReadMemStats(&ms2)
		collectResult("Badger-IterStress-Latest", b.N, dur, ms1, ms2)
	})

	b.Run("DualTimelinePebble-Stress-Iterator", func(b *testing.B) {
		b.ReportAllocs()

		// Pre-populate with many versions
		store, err := NewDualTimelinePebbleStore("", 1)
		require.NoError(b, err)
		defer store.Close()

		b.Logf("Populating %d versions with %d keys each...", stressVersions, stressKeys)
		for v := 1; v <= stressVersions; v++ {
			store.version = uint64(v)
			store.batch = store.db.NewBatch()

			for j := 0; j < stressKeys; j++ {
				require.NoError(b, store.Set(keys[j], []byte(fmt.Sprintf("value_v%d_%d", v, j))))
			}
			require.NoError(b, store.Commit())
            if progressEnabled() {
                fmt.Printf("[Progress] DualTimeline populate v %d/%d (%d keys)\n", v, stressVersions, stressKeys)
            }
		}
        // Optional: quiesce LSS range before timing latest to emulate steady-state
        if quiesceEnabled() {
            _ = store.db.Flush()
            _ = store.db.Compact([]byte("s/"), []byte("t/"), true)
        }
		b.ResetTimer()
		var ms1, ms2 runtime.MemStats
		runtime.ReadMemStats(&ms1)
		t0 := time.Now()
		for i := 0; i < b.N; i++ {
			it, err := store.Iterator(nil)
			require.NoError(b, err)

			count := 0
			for it.Valid() {
				_ = it.Key()
				_ = it.Value()
				it.Next()
				count++
			}
			it.Close()

			if count != stressKeys {
				b.Fatalf("Expected %d keys, got %d", stressKeys, count)
			}
		}
		dur := time.Since(t0)
		runtime.ReadMemStats(&ms2)
		collectResult("DualTimelinePebble-IterStress-Latest", b.N, dur, ms1, ms2)
	})

	b.Run("SplitTimelinePebble-Stress-Iterator", func(b *testing.B) {
		b.ReportAllocs()

		// Pre-populate with many versions
		store, err := NewSplitTimelinePebbleStore("", "", 1)
		require.NoError(b, err)
		defer store.Close()

		b.Logf("Populating %d versions with %d keys each...", stressVersions, stressKeys)
		for v := 1; v <= stressVersions; v++ {
			store.version = uint64(v)
			store.hssBatch = store.hssDB.NewBatch()
			store.lssBatch = store.lssDB.NewBatch()

			for j := 0; j < stressKeys; j++ {
				require.NoError(b, store.Set(keys[j], []byte(fmt.Sprintf("value_v%d_%d", v, j))))
			}
			require.NoError(b, store.Commit())
            if progressEnabled() {
                fmt.Printf("[Progress] SplitTimeline populate v %d/%d (%d keys)\n", v, stressVersions, stressKeys)
            }
		}
        // Optional: quiesce LSS range before timing latest to emulate steady-state
        if quiesceEnabled() {
            _ = store.lssDB.Flush()
            _ = store.lssDB.Compact([]byte("s/"), []byte("t/"), true)
        }
		b.ResetTimer()
		var ms1, ms2 runtime.MemStats
		runtime.ReadMemStats(&ms1)
		t0 := time.Now()
		for i := 0; i < b.N; i++ {
			it, err := store.Iterator(nil)
			require.NoError(b, err)

			count := 0
			for it.Valid() {
				_ = it.Key()
				_ = it.Value()
				it.Next()
				count++
			}
			it.Close()

			if count != stressKeys {
				b.Fatalf("Expected %d keys, got %d", stressKeys, count)
			}
		}
		dur := time.Since(t0)
		runtime.ReadMemStats(&ms2)
		collectResult("SplitTimelinePebble-IterStress-Latest", b.N, dur, ms1, ms2)
	})

	// Print summary table for this benchmark
	printBenchmarkTable(benchResults, "IterationStressTest Results")
}

func Benchmark_SystemLevel_FullTest(b *testing.B) {
	// Generate keys
	keys := make([][]byte, numKeys)
	if keyShape == "realistic" {
		for i := 0; i < numKeys; i++ {
			switch i % 4 {
			case 0: // account_
				addr := make([]byte, 20)
				binary.BigEndian.PutUint64(addr[12:], uint64(i))
				keys[i] = append([]byte("account_"), addr...)
			case 1: // validator_
				addr := make([]byte, 20)
				binary.BigEndian.PutUint64(addr[12:], uint64(i))
				keys[i] = append([]byte("validator_"), addr...)
			case 2: // committee_
				addr := make([]byte, 20)
				binary.BigEndian.PutUint64(addr[12:], uint64(i))
				chainId := make([]byte, 8)
				binary.BigEndian.PutUint64(chainId, uint64(i%10))
				stake := make([]byte, 8)
				binary.BigEndian.PutUint64(stake, uint64(1000+i))
				keys[i] = append(append(append([]byte("committee_"), chainId...), stake...), addr...)
			default: // hash_
				sum := sha256.Sum256([]byte(fmt.Sprintf("hash_input_%d", i)))
				keys[i] = append([]byte("hash_"), sum[:]...)
			}
		}
	} else {
		for i := 0; i < numKeys; i++ {
			keys[i] = []byte(fmt.Sprintf("%d", i))
		}
	}

	for _, versions := range versionsList {
		versionKey := fmt.Sprintf("v%d", versions)

		b.Run(versionKey, func(b *testing.B) {
			beginBenchCollection()
			b.Run("Badger-System-Write", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					store, err := NewBadgerVersionedStore("", 1)
					require.NoError(b, err)

					b.StartTimer()
					var ms1, ms2 runtime.MemStats
					runtime.ReadMemStats(&ms1)
					t0 := time.Now()
					for v := 1; v <= versions; v++ {
						store.version = uint64(v)
						store.hssBatch = store.db.NewWriteBatchAt(uint64(v))
						store.lssBatch = store.db.NewWriteBatchAt(lssVersion)

						payload := make([]byte, valueBytes)
						for j := 0; j < numKeys; j++ {
							require.NoError(b, store.Set(keys[j], payload))
						}
						require.NoError(b, store.Commit())
					}
					b.StopTimer()
					dur := time.Since(t0)
					runtime.ReadMemStats(&ms2)
					collectResult("Badger-System-Write", b.N, dur, ms1, ms2)

					store.Close()
				}
				// Note: Detailed metrics will be captured by running sub-benchmarks
				// The summary table will be printed at the end
			})

			b.Run("Badger-System-Latest-Iterator", func(b *testing.B) {
				b.ReportAllocs()
				// Pre-populate data
				store, err := NewBadgerVersionedStore("", 1)
				require.NoError(b, err)

				for v := 1; v <= versions; v++ {
					store.version = uint64(v)
					store.hssBatch = store.db.NewWriteBatchAt(uint64(v))
					store.lssBatch = store.db.NewWriteBatchAt(lssVersion)

					payload := make([]byte, valueBytes)
					for j := 0; j < numKeys; j++ {
						require.NoError(b, store.Set(keys[j], payload))
					}
					require.NoError(b, store.Commit())
				}

				b.ResetTimer()
				var ms1, ms2 runtime.MemStats
				runtime.ReadMemStats(&ms1)
				t0 := time.Now()
				for i := 0; i < b.N; i++ {
					it, err := store.Iterator(nil)
					require.NoError(b, err)

					count := 0
					for it.Valid() {
						it.Key()
						it.Value()
						it.Next()
						count++
					}
					it.Close()
				}
				dur := time.Since(t0)
				runtime.ReadMemStats(&ms2)
				collectResult("Badger-System-Latest-Iterator", b.N, dur, ms1, ms2)

				store.Close()
			})

			b.Run("DualTimelinePebble-Historical-Iterator", func(b *testing.B) {
				b.ReportAllocs()
				// Pre-populate data across versions
				store, err := NewDualTimelinePebbleStore("", 1)
				require.NoError(b, err)

				for v := 1; v <= versions; v++ {
					store.version = uint64(v)
					store.batch = store.db.NewBatch()

					payload := make([]byte, valueBytes)
					for j := 0; j < numKeys; j++ {
						require.NoError(b, store.Set(keys[j], payload))
					}
					require.NoError(b, store.Commit())
				}

				// Iterate historical view at version 1
				histVersion := uint64(1)
				b.ResetTimer()
				var ms1, ms2 runtime.MemStats
				runtime.ReadMemStats(&ms1)
				t0 := time.Now()
				for i := 0; i < b.N; i++ {
					lb, ub := boundsForHeight(histVersion)
					it, err := store.db.NewIter(&pebble.IterOptions{LowerBound: lb, UpperBound: ub})
					require.NoError(b, err)

					count := 0
					for it.First(); it.Valid(); it.Next() {
						_ = it.Key()
						_ = it.Value()
						count++
					}
					it.Close()
				}
				dur := time.Since(t0)
				runtime.ReadMemStats(&ms2)
				collectResult("DualTimelinePebble-Historical-Iterator", b.N, dur, ms1, ms2)

				store.Close()
			})

			// SplitTimeline Pebble: Historical view using height-bounded scan over HSS DB
			b.Run("SplitTimelinePebble-Historical-Iterator", func(b *testing.B) {
				b.ReportAllocs()
				// Pre-populate data across versions
				store, err := NewSplitTimelinePebbleStore("", "", 1)
				require.NoError(b, err)

				for v := 1; v <= versions; v++ {
					store.version = uint64(v)
					store.hssBatch = store.hssDB.NewBatch()
					store.lssBatch = store.lssDB.NewBatch()

					payload := make([]byte, valueBytes)
					for j := 0; j < numKeys; j++ {
						require.NoError(b, store.Set(keys[j], payload))
					}
					require.NoError(b, store.Commit())
				}

				// Iterate historical view at version 1
				histVersion := uint64(1)
				b.ResetTimer()
				var ms1, ms2 runtime.MemStats
				runtime.ReadMemStats(&ms1)
				t0 := time.Now()
				for i := 0; i < b.N; i++ {
					lb, ub := boundsForHeight(histVersion)
					it, err := store.hssDB.NewIter(&pebble.IterOptions{LowerBound: lb, UpperBound: ub})
					require.NoError(b, err)

					count := 0
					for it.First(); it.Valid(); it.Next() {
						_ = it.Key()
						_ = it.Value()
						count++
					}
					it.Close()
				}
				dur := time.Since(t0)
				runtime.ReadMemStats(&ms2)
				collectResult("SplitTimelinePebble-Historical-Iterator", b.N, dur, ms1, ms2)

				store.Close()
			})

			// SeekLT Pebble: Historical view using per-key SeekLT lookups at a fixed version (e.g., 1)
			b.Run("SeekLTPebble-Historical-SeekLT", func(b *testing.B) {
				b.ReportAllocs()
				// Pre-populate data using SeekLT store
				store, err := NewSeekLTPebbleStore("", 1)
				require.NoError(b, err)

				for v := 1; v <= versions; v++ {
					store.version = uint64(v)
					payload := make([]byte, valueBytes)
					for j := 0; j < numKeys; j++ {
						require.NoError(b, store.Set(keys[j], payload))
					}
					require.NoError(b, store.Commit())
				}

				// Set to a historical version (e.g., 1)
				histVersion := uint64(1)
				store.version = histVersion

				b.ResetTimer()
				var ms1, ms2 runtime.MemStats
				runtime.ReadMemStats(&ms1)
				t0 := time.Now()
				for i := 0; i < b.N; i++ {
					count := 0
					for _, k := range keys {
						if val, err := store.Get(k); err == nil && val != nil {
							count++
						}
					}
					if count != len(keys) {
						b.Fatalf("unexpected count: %d", count)
					}
				}
				dur := time.Since(t0)
				runtime.ReadMemStats(&ms2)
				collectResult("SeekLTPebble-Historical-SeekLT", b.N, dur, ms1, ms2)

				store.Close()
			})

			// SeekLT Pebble: Write performance across versions
			b.Run("SeekLTPebble-Write", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					store, err := NewSeekLTPebbleStore("", 1)
					require.NoError(b, err)

					b.StartTimer()
					var ms1, ms2 runtime.MemStats
					runtime.ReadMemStats(&ms1)
					t0 := time.Now()
					for v := 1; v <= versions; v++ {
						store.version = uint64(v)
						payload := make([]byte, valueBytes)
						for _, key := range keys {
							require.NoError(b, store.Set(key, payload))
						}
						require.NoError(b, store.Commit())
					}
					b.StopTimer()
					dur := time.Since(t0)
					runtime.ReadMemStats(&ms2)
					collectResult("SeekLTPebble-Write", b.N, dur, ms1, ms2)

					store.Close()
				}
			})

			// SeekLT Pebble: Latest lookup via per-key SeekLT
			b.Run("SeekLTPebble-Latest-SeekLT", func(b *testing.B) {
				b.ReportAllocs()
				// Pre-populate data using SeekLT store
				store, err := NewSeekLTPebbleStore("", 1)
				require.NoError(b, err)

				for v := 1; v <= versions; v++ {
					store.version = uint64(v)
					payload := make([]byte, valueBytes)
					for j := 0; j < numKeys; j++ {
						require.NoError(b, store.Set(keys[j], payload))
					}
					require.NoError(b, store.Commit())
				}

				// Reset to latest version for reads
				store.version = uint64(versions)
				// Optional: quiesce entire range before timing latest GETs
				if quiesceEnabled() {
					_ = store.db.Flush()
					_ = store.db.Compact(nil, nil, true)
				}
				b.ResetTimer()
				var ms1, ms2 runtime.MemStats
				runtime.ReadMemStats(&ms1)
				t0 := time.Now()
				for i := 0; i < b.N; i++ {
					count := 0
					for _, k := range keys {
						if val, err := store.Get(k); err == nil && val != nil {
							count++
						}
					}
					if count != len(keys) {
						b.Fatalf("unexpected count: %d", count)
					}
				}
				dur := time.Since(t0)
				runtime.ReadMemStats(&ms2)
				collectResult("SeekLTPebble-Latest-SeekLT", b.N, dur, ms1, ms2)

				store.Close()
			})

			b.Run("Badger-System-Historical-Iterator", func(b *testing.B) {
				b.ReportAllocs()
				// Pre-populate data across versions
				store, err := NewBadgerVersionedStore("", 1)
				require.NoError(b, err)

				for v := 1; v <= versions; v++ {
					store.version = uint64(v)
					store.hssBatch = store.db.NewWriteBatchAt(uint64(v))
					store.lssBatch = store.db.NewWriteBatchAt(lssVersion)

					payload := make([]byte, valueBytes)
					for j := 0; j < numKeys; j++ {
						require.NoError(b, store.Set(keys[j], payload))
					}
					require.NoError(b, store.Commit())
				}

				// Iterate historical view at version 1 (matches canopy HSS read path)
				histVersion := uint64(1)
				b.ResetTimer()
				var ms1, ms2 runtime.MemStats
				runtime.ReadMemStats(&ms1)
				t0 := time.Now()
				for i := 0; i < b.N; i++ {
					it, err := store.HistoricalIterator(histVersion, nil)
					require.NoError(b, err)

					count := 0
					for it.Valid() {
						it.Key()
						it.Value()
						it.Next()
						count++
					}
					it.Close()
				}
				dur := time.Since(t0)
				runtime.ReadMemStats(&ms2)
				collectResult("Badger-System-Historical-Iterator", b.N, dur, ms1, ms2)

				store.Close()
			})

			b.Run("DualTimelinePebble-Write", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					store, err := NewDualTimelinePebbleStore("", 1)
					require.NoError(b, err)

					b.StartTimer()
					var ms1, ms2 runtime.MemStats
					runtime.ReadMemStats(&ms1)
					t0 := time.Now()
					for v := 1; v <= versions; v++ {
						store.version = uint64(v)
						store.batch = store.db.NewBatch()

						payload := make([]byte, valueBytes)
						for j := 0; j < numKeys; j++ {
							require.NoError(b, store.Set(keys[j], payload))
						}
						require.NoError(b, store.Commit())
					}
					b.StopTimer()
					dur := time.Since(t0)
					runtime.ReadMemStats(&ms2)
					collectResult("DualTimelinePebble-Write", b.N, dur, ms1, ms2)

					store.Close()
				}
			})

			b.Run("SplitTimelinePebble-Write", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					store, err := NewSplitTimelinePebbleStore("", "", 1)
					require.NoError(b, err)

					b.StartTimer()
					var ms1, ms2 runtime.MemStats
					runtime.ReadMemStats(&ms1)
					t0 := time.Now()
					for v := 1; v <= versions; v++ {
						store.version = uint64(v)
						store.hssBatch = store.hssDB.NewBatch()
						store.lssBatch = store.lssDB.NewBatch()

						payload := make([]byte, valueBytes)
						for j := 0; j < numKeys; j++ {
							require.NoError(b, store.Set(keys[j], payload))
						}
						require.NoError(b, store.Commit())
					}
					b.StopTimer()
					dur := time.Since(t0)
					runtime.ReadMemStats(&ms2)
					collectResult("SplitTimelinePebble-Write", b.N, dur, ms1, ms2)

					store.Close()
				}
			})

			b.Run("DualTimelinePebble-Latest-Iterator", func(b *testing.B) {
				b.ReportAllocs()
				// Pre-populate data
				store, err := NewDualTimelinePebbleStore("", 1)
				require.NoError(b, err)

				for v := 1; v <= versions; v++ {
					store.version = uint64(v)
					store.batch = store.db.NewBatch()

					payload := make([]byte, valueBytes)
					for j := 0; j < numKeys; j++ {
						require.NoError(b, store.Set(keys[j], payload))
					}
					require.NoError(b, store.Commit())
				}
				// Optional: quiesce latest range before timing
				if quiesceEnabled() {
					_ = store.db.Flush()
					_ = store.db.Compact([]byte("s/"), []byte("t/"), true)
				}
				b.ResetTimer()
				var ms1, ms2 runtime.MemStats
				runtime.ReadMemStats(&ms1)
				t0 := time.Now()
				for i := 0; i < b.N; i++ {
					it, err := store.Iterator(nil)
					require.NoError(b, err)

					count := 0
					for it.Valid() {
						it.Key()
						it.Value()
						it.Next()
						count++
					}
					it.Close()
				}
				dur := time.Since(t0)
				runtime.ReadMemStats(&ms2)
				collectResult("DualTimelinePebble-Latest-Iterator", b.N, dur, ms1, ms2)

				store.Close()
			})

			b.Run("SplitTimelinePebble-Latest-Iterator", func(b *testing.B) {
				b.ReportAllocs()
				// Pre-populate data
				store, err := NewSplitTimelinePebbleStore("", "", 1)
				require.NoError(b, err)

				for v := 1; v <= versions; v++ {
					store.version = uint64(v)
					store.hssBatch = store.hssDB.NewBatch()
					store.lssBatch = store.lssDB.NewBatch()

					payload := make([]byte, valueBytes)
					for j := 0; j < numKeys; j++ {
						require.NoError(b, store.Set(keys[j], payload))
					}
					require.NoError(b, store.Commit())
				}
				// Optional: quiesce latest range before timing
				if quiesceEnabled() {
					_ = store.lssDB.Flush()
					_ = store.lssDB.Compact([]byte("s/"), []byte("t/"), true)
				}
				b.ResetTimer()
				var ms1, ms2 runtime.MemStats
				runtime.ReadMemStats(&ms1)
				t0 := time.Now()
				for i := 0; i < b.N; i++ {
					it, err := store.Iterator(nil)
					require.NoError(b, err)

					count := 0
					for it.Valid() {
						it.Key()
						it.Value()
						it.Next()
						count++
					}
					it.Close()
				}
				dur := time.Since(t0)
				runtime.ReadMemStats(&ms2)
				collectResult("SplitTimelinePebble-Latest-Iterator", b.N, dur, ms1, ms2)

				store.Close()
			})

			// SeekLT Pebble: Historical iterator over all keys at a given version
			b.Run("SeekLTPebble-Historical-Iterator", func(b *testing.B) {
				b.ReportAllocs()
				store, err := NewSeekLTPebbleStore("", 1)
				require.NoError(b, err)

				for v := 1; v <= versions; v++ {
					store.version = uint64(v)
					payload := make([]byte, valueBytes)
					for j := 0; j < numKeys; j++ {
						require.NoError(b, store.Set(keys[j], payload))
					}
					require.NoError(b, store.Commit())
				}

				histVersion := uint64(1)
				store.version = histVersion
				b.ResetTimer()
				var ms1, ms2 runtime.MemStats
				runtime.ReadMemStats(&ms1)
				t0 := time.Now()
				for i := 0; i < b.N; i++ {
					it, err := store.Iterator(nil)
					require.NoError(b, err)
					count := 0
					for it.Valid() {
						_ = it.Key()
						_ = it.Value()
						it.Next()
						count++
					}
					it.Close()
					if count != numKeys {
						b.Fatalf("unexpected count: %d", count)
					}
				}
				dur := time.Since(t0)
				runtime.ReadMemStats(&ms2)
				collectResult("SeekLTPebble-Historical-Iterator", b.N, dur, ms1, ms2)

				store.Close()
			})

			// Print summary table for this versioned system-level run
			printBenchmarkTable(benchResults, fmt.Sprintf("SystemLevel Results %s", versionKey))
		})

	}

}

// Benchmark_Write_Performance measures write performance across stores with a smaller keyset
func Benchmark_Write_Performance(b *testing.B) {
	beginBenchCollection()
	keys := make([][]byte, 10000)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("key-%06d", i))
	}

	stores := []struct {
		name    string
		factory func() (VersionedStore, error)
	}{
		{"Badger", func() (VersionedStore, error) { return NewBadgerVersionedStore("", 1) }},
		{"DualTimelinePebble", func() (VersionedStore, error) { return NewDualTimelinePebbleStore("", 1) }},
		{"SplitTimelinePebble", func() (VersionedStore, error) { return NewSplitTimelinePebbleStore("", "", 1) }},
		{"SeekLTPebble", func() (VersionedStore, error) { return NewSeekLTPebbleStore("", 1) }},
	}

	for _, s := range stores {
		b.Run(s.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				store, err := s.factory()
				require.NoError(b, err)

				b.StartTimer()
				var ms1, ms2 runtime.MemStats
				runtime.ReadMemStats(&ms1)
				t0 := time.Now()
				for _, key := range keys {
					require.NoError(b, store.Set(key, key))
				}
				require.NoError(b, store.Commit())
				b.StopTimer()
				dur := time.Since(t0)
				runtime.ReadMemStats(&ms2)
				collectResult(s.name, b.N, dur, ms1, ms2)

				store.Close()
			}
		})
	}

	// Print summary table for write performance benchmark
	printBenchmarkTable(benchResults, "Write Performance Results")
}
