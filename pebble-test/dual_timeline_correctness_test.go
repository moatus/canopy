package main

import "testing"

// TestSystemCorrectness_DualTimelinePebble runs system-level correctness against the Dual Timeline Pebble store
func TestSystemCorrectness_DualTimelinePebble(t *testing.T) {
	runSystemCorrectness(t, func() (VersionedStore, error) { return NewDualTimelinePebbleStore("", 1) }, "DualTimelinePebble")
}
