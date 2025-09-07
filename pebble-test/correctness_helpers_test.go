package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// runSystemCorrectness is a shared helper that validates basic Set/Get/Commit behavior
func runSystemCorrectness(t *testing.T, factory func() (VersionedStore, error), name string) {
	t.Helper()
	testKey := []byte("testkey")
	testValue1 := []byte("value1")
	testValue2 := []byte("value2")

	store, err := factory()
	require.NoError(t, err)
	defer store.Close()

	// Test Set/Get
	require.NoError(t, store.Set(testKey, testValue1))
	require.NoError(t, store.Commit())

	val, err := store.Get(testKey)
	require.NoError(t, err)
	require.Equal(t, testValue1, val)

	// Test Update
	require.NoError(t, store.Set(testKey, testValue2))
	require.NoError(t, store.Commit())

	val, err = store.Get(testKey)
	require.NoError(t, err)
	require.Equal(t, testValue2, val)

	t.Logf("\u2713 %s system correctness validated", name)
}
