package main

import "testing"

// TestSystemCorrectness_SeekLTPebble runs system-level correctness against the SeekLT Pebble store (Issue-196 style)
func TestSystemCorrectness_SeekLTPebble(t *testing.T) {
	runSystemCorrectness(t, func() (VersionedStore, error) { return NewSeekLTPebbleStore("", 1) }, "SeekLTPebble")
}
