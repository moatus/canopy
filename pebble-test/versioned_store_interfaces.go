package main

// Shared interfaces for versioned store benchmarks.
// Moved from benchmark_test.go so non-test stores (e.g., split timeline) can compile without
// depending on _test-defined symbols.

// VersionedStore is the minimal API exercised by the system-level benchmarks.
type VersionedStore interface {
	Set(key, value []byte) error
	Get(key []byte) ([]byte, error)
	Iterator(prefix []byte) (Iterator, error)
	Commit() error
	Close() error
}

// Iterator provides a forward-only iterator over a keyspace.
type Iterator interface {
	Valid() bool
	Next()
	Key() []byte
	Value() []byte
	Close()
}
