package main

import "testing"

// TestSystemCorrectness_SplitTimelinePebble runs system-level correctness against the Split Timeline Pebble store
func TestSystemCorrectness_SplitTimelinePebble(t *testing.T) {
	runSystemCorrectness(t, func() (VersionedStore, error) { return NewSplitTimelinePebbleStore("", "", 1) }, "SplitTimelinePebble")
}
