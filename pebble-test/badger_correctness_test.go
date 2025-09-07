package main

import "testing"

// TestSystemCorrectness_Badger runs system-level correctness against the Badger store
func TestSystemCorrectness_Badger(t *testing.T) {
	runSystemCorrectness(t, func() (VersionedStore, error) { return NewBadgerVersionedStore("", 1) }, "Badger")
}
