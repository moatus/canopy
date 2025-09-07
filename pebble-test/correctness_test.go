package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// parityIterator collects all key/value pairs from an Iterator into deterministic slices.
func parityIterator(t *testing.T, it Iterator) (keys [][]byte, values [][]byte) {
	t.Helper()
	defer it.Close()
	for it.Valid() {
		k := append([]byte(nil), it.Key()...)
		v := append([]byte(nil), it.Value()...)
		keys = append(keys, k)
		values = append(values, v)
		it.Next()
	}
	return
}

// assertKVEqual compares two key/value sequences for equality.
func assertKVEqual(t *testing.T, aK, aV, bK, bV [][]byte, whoA, whoB string) {
	t.Helper()
	require.Equal(t, len(aK), len(bK), "key count mismatch between %s and %s", whoA, whoB)
	require.Equal(t, len(aV), len(bV), "value count mismatch between %s and %s", whoA, whoB)
	for i := range aK {
		if !bytes.Equal(aK[i], bK[i]) || !bytes.Equal(aV[i], bV[i]) {
			t.Fatalf("mismatch at index %d: %s vs %s", i, whoA, whoB)
		}
	}
}

// genDeterministicKeys returns N deterministic, realistic-shaped Canopy keys.
func genDeterministicKeys(n int) [][]byte {
	keys := make([][]byte, n)
	for i := 0; i < n; i++ {
		switch i % 4 {
		case 0:
			addr := make([]byte, 20)
			binary.BigEndian.PutUint64(addr[12:], uint64(i))
			keys[i] = append([]byte("account_"), addr...)
		case 1:
			addr := make([]byte, 20)
			binary.BigEndian.PutUint64(addr[12:], uint64(i))
			keys[i] = append([]byte("validator_"), addr...)
		case 2:
			addr := make([]byte, 20)
			binary.BigEndian.PutUint64(addr[12:], uint64(i))
			chainId := make([]byte, 8)
			binary.BigEndian.PutUint64(chainId, uint64(i%10))
			stake := make([]byte, 8)
			binary.BigEndian.PutUint64(stake, uint64(1000+i))
			keys[i] = append(append(append([]byte("committee_"), chainId...), stake...), addr...)
		case 3:
			h := sha256.Sum256([]byte(fmt.Sprintf("hash_input_%d", i)))
			keys[i] = append([]byte("hash_"), h[:]...)
		}
	}
	return keys
}

// Test_Parity_Sanity performs a small, fast correctness cross-check that mirrors the fuzz test
// intent, but across the four system variants used by benchmarks. It validates that "latest"
// iterations yield the same key/value sequences for a small dataset, and that a historical
// view (at a mid version) matches among the variants that support it.
func Test_Parity_Sanity(t *testing.T) {
	const (
		numKeys     = 256
		numVersions = 5
	)

	keys := genDeterministicKeys(numKeys)

	// Create stores
	badgerStore, err := NewBadgerVersionedStore("", 1)
	require.NoError(t, err)
	defer badgerStore.Close()

	dualStore, err := NewDualTimelinePebbleStore("", 1)
	require.NoError(t, err)
	defer dualStore.Close()

	splitStore, err := NewSplitTimelinePebbleStore("", "", 1)
	require.NoError(t, err)
	defer splitStore.Close()

	seekltStore, err := NewSeekLTPebbleStore("", 1)
	require.NoError(t, err)
	defer seekltStore.Close()

	// Populate the same sequence into all stores
	for v := 1; v <= numVersions; v++ {
		payload := []byte(fmt.Sprintf("v%d", v))
		// Badger
		badgerStore.version = uint64(v)
		badgerStore.hssBatch = badgerStore.db.NewWriteBatchAt(uint64(v))
		badgerStore.lssBatch = badgerStore.db.NewWriteBatchAt(lssVersion)
		for _, k := range keys {
			require.NoError(t, badgerStore.Set(k, payload))
		}
		require.NoError(t, badgerStore.Commit())

		// Dual
		dualStore.version = uint64(v)
		dualStore.batch = dualStore.db.NewBatch()
		for _, k := range keys {
			require.NoError(t, dualStore.Set(k, payload))
		}
		require.NoError(t, dualStore.Commit())

		// Split
		splitStore.version = uint64(v)
		splitStore.hssBatch = splitStore.hssDB.NewBatch()
		splitStore.lssBatch = splitStore.lssDB.NewBatch()
		for _, k := range keys {
			require.NoError(t, splitStore.Set(k, payload))
		}
		require.NoError(t, splitStore.Commit())

		// SeekLT
		seekltStore.version = uint64(v)
		seekltStore.batch = seekltStore.db.NewIndexedBatch()
		for _, k := range keys {
			require.NoError(t, seekltStore.Set(k, payload))
		}
		require.NoError(t, seekltStore.Commit())
	}

	// LATEST parity: all variants should surface exactly one KV per logical key, equal order/values
	itB, err := badgerStore.Iterator(nil)
	require.NoError(t, err)
	bK, bV := parityIterator(t, itB)
	itD, err := dualStore.Iterator(nil)
	require.NoError(t, err)
	dK, dV := parityIterator(t, itD)
	itS, err := splitStore.Iterator(nil)
	require.NoError(t, err)
	sK, sV := parityIterator(t, itS)
	itL, err := seekltStore.Iterator(nil)
	require.NoError(t, err)
	lK, lV := parityIterator(t, itL)

	// Expected count
	require.Equal(t, numKeys, len(bK))
	require.Equal(t, numKeys, len(dK))
	require.Equal(t, numKeys, len(sK))
	require.Equal(t, numKeys, len(lK))

	// Pairwise equality
	assertKVEqual(t, bK, bV, dK, dV, "Badger", "DualTimelinePebble")
	assertKVEqual(t, bK, bV, sK, sV, "Badger", "SplitTimelinePebble")
	assertKVEqual(t, bK, bV, lK, lV, "Badger", "SeekLTPebble")

	// HISTORICAL parity (mid version): compare those variants that expose a historical iterator API
	mid := uint64((numVersions + 1) / 2)
	bit, err := badgerStore.HistoricalIterator(mid, nil)
	require.NoError(t, err)
	bk, bv := parityIterator(t, bit)
	// SplitTimeline supports HistoricalIterator directly
	pit, err := splitStore.HistoricalIterator(mid, nil)
	require.NoError(t, err)
	pk, pv := parityIterator(t, pit)
	// SeekLT: set the visible version and iterate
	seekltStore.version = mid
	lit, err := seekltStore.Iterator(nil)
	require.NoError(t, err)
	lk, lv := parityIterator(t, lit)

	// Counts
	require.Equal(t, numKeys, len(bk))
	require.Equal(t, numKeys, len(pk))
	require.Equal(t, numKeys, len(lk))
	// Equality vs Badger
	assertKVEqual(t, bk, bv, pk, pv, "Badger@mid", "SplitTimelinePebble@mid")
	assertKVEqual(t, bk, bv, lk, lv, "Badger@mid", "SeekLTPebble@mid")
}
