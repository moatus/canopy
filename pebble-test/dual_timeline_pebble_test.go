package main

import (
    "fmt"
    "testing"

    "github.com/cockroachdb/pebble/v2"
    "github.com/cockroachdb/pebble/v2/vfs"
    "github.com/stretchr/testify/require"
)

// Test_DualTimelinePebble_LSS_HSS_Reads validates LSS/HSS correctness
func Test_DualTimelinePebble_LSS_HSS_Reads(t *testing.T) {
    db, err := pebble.Open("", &pebble.Options{FS: vfs.NewMem(), DisableWAL: true})
    require.NoError(t, err)
    defer db.Close()

    // Simulate two height commits
    key1 := []byte("testkey1")
    value1 := []byte("value_at_height_1")
    value2 := []byte("value_at_height_2")

    // Height 1: Write to both LSS and HSS
    lssKey := keyLSS(key1)
    hssKey1 := keyHSS(1, key1)
    
    batch := db.NewBatch()
    require.NoError(t, batch.Set(lssKey, value1, nil))
    require.NoError(t, batch.Set(hssKey1, value1, nil))
    require.NoError(t, batch.Commit(pebble.NoSync))

    // Height 2: Update LSS and write to HSS
    hssKey2 := keyHSS(2, key1)
    batch = db.NewBatch()
    require.NoError(t, batch.Set(lssKey, value2, nil)) // overwrite LSS
    require.NoError(t, batch.Set(hssKey2, value2, nil))
    require.NoError(t, batch.Commit(pebble.NoSync))

    // Test latest read from LSS
    val, closer, err := db.Get(lssKey)
    require.NoError(t, err)
    defer closer.Close()
    require.Equal(t, value2, val)

    // Test historical read at height 1
    val1, closer1, err := db.Get(hssKey1)
    require.NoError(t, err)
    defer closer1.Close()
    require.Equal(t, value1, val1)

    // Test historical read at height 2
    val2, closer2, err := db.Get(hssKey2)
    require.NoError(t, err)
    defer closer2.Close()
    require.Equal(t, value2, val2)

    t.Logf("✓ LSS/HSS reads working correctly")
}

// Benchmark_DualTimelinePebble_vs_Issue196 compares Dual Timeline Pebble performance against versioned approach
func Benchmark_DualTimelinePebble_vs_Issue196(b *testing.B) {
    const numKeys = 200_000
    const numVers = 4

    // Pebble in-mem for Dual Timeline Pebble
    db, err := pebble.Open("", &pebble.Options{FS: vfs.NewMem(), DisableWAL: true})
    if err != nil {
        b.Fatalf("open pebble: %v", err)
    }
    defer db.Close()

    // Pre-generate keys
    keys := make([][]byte, numKeys)
    for i := range keys {
        keys[i] = []byte(fmt.Sprintf("key-%06d", i))
    }

    // Populate Dual Timeline Pebble layout: s/<key> latest, h/<H>/<key> historical
    for h := 1; h <= numVers; h++ {
        batch := db.NewBatch()
        for _, k := range keys {
            // write to historical partition
            if err := batch.Set(keyHSS(uint64(h), k), k, nil); err != nil {
                b.Fatal(err)
            }
            // write to latest (overwrite)
            if err := batch.Set(keyLSS(k), k, nil); err != nil {
                b.Fatal(err)
            }
        }
        if err := batch.Commit(pebble.NoSync); err != nil {
            b.Fatal(err)
        }
    }

    // Issue-196 style versioned keys: [userKey][8B ver][1B tomb]
    db2, err := pebble.Open("", &pebble.Options{FS: vfs.NewMem(), DisableWAL: true})
    if err != nil {
        b.Fatalf("open pebble2: %v", err)
    }
    defer db2.Close()

    for h := 1; h <= numVers; h++ {
        batch := db2.NewBatch()
        for _, k := range keys {
            vkey := versionedKey(k, uint64(h), false)
            if err := batch.Set(vkey, k, nil); err != nil {
                b.Fatal(err)
            }
        }
        if err := batch.Commit(pebble.NoSync); err != nil {
            b.Fatal(err)
        }
    }

    // Run parameterized sub-benchmarks for two sample sizes to show scaling
    for _, sample := range []int{1_000, 200_000} {
        // Ensure sample does not exceed total keys
        if sample > numKeys { sample = numKeys }

        b.Run(fmt.Sprintf("DualTimelinePebble-Latest-Iter/%d", sample), func(b *testing.B) {
            b.ReportAllocs()
            for i := 0; i < b.N; i++ {
                it, _ := db.NewIter(&pebble.IterOptions{LowerBound: []byte("s/"), UpperBound: []byte("t/")})
                cnt := 0
                for it.First(); it.Valid(); it.Next() {
                    _ = it.Key()
                    _ = it.Value()
                    cnt++
                    if sample < numKeys && cnt >= sample { break }
                }
                it.Close()
                if sample == numKeys && cnt != numKeys {
                    b.Fatalf("unexpected count: %d", cnt)
                }
                if sample < numKeys && cnt != sample {
                    b.Fatalf("unexpected sample count: %d", cnt)
                }
            }
        })

        b.Run(fmt.Sprintf("DualTimelinePebble-Historical-Iter/%d", sample), func(b *testing.B) {
            b.ReportAllocs()
            H := uint64(numVers - 1)
            for i := 0; i < b.N; i++ {
                lb, ub := boundsForHeight(H)
                it, _ := db.NewIter(&pebble.IterOptions{LowerBound: lb, UpperBound: ub})
                cnt := 0
                for it.First(); it.Valid(); it.Next() {
                    _ = it.Key()
                    _ = it.Value()
                    cnt++
                    if sample < numKeys && cnt >= sample { break }
                }
                it.Close()
                if sample == numKeys && cnt != numKeys {
                    b.Fatalf("unexpected count: %d", cnt)
                }
                if sample < numKeys && cnt != sample {
                    b.Fatalf("unexpected sample count: %d", cnt)
                }
            }
        })

        b.Run(fmt.Sprintf("SeekLTPebble-Latest-SeekLT/%d", sample), func(b *testing.B) {
            b.ReportAllocs()
            
            // Create SeekLT Pebble versioned store for this benchmark
            seekltStore, err := NewSeekLTPebbleStore("", uint64(numVers))
            if err != nil {
                b.Fatal(err)
            }
            defer seekltStore.Close()
            
            // Populate SeekLT store with the same data
            for v := 1; v <= numVers; v++ {
                seekltStore.version = uint64(v)
                for _, k := range keys {
                    val := fmt.Sprintf("value-%d-%s", v, string(k))
                    seekltStore.Set(k, []byte(val))
                }
                seekltStore.Commit()
            }
            
            // Reset to latest version for reads
            seekltStore.version = uint64(numVers)
            
            for i := 0; i < b.N; i++ {
                cnt := 0
                // Use SeekLT Get method which uses SeekLT internally
                for _, k := range keys[:sample] {
                    val, err := seekltStore.Get(k)
                    if err == nil && val != nil {
                        cnt++
                    }
                }
                if cnt != sample {
                    b.Fatalf("unexpected sample count: %d", cnt)
                }
            }
        })
    }
}

// Helper function for versioned keys (issue-#196 style)
func versionedKey(user []byte, ver uint64, tomb bool) []byte {
    k := make([]byte, 0, len(user)+9)
    k = append(k, user...)
    var hb [8]byte
    for i := 0; i < 8; i++ {
        hb[7-i] = byte(ver)
        ver >>= 8
    }
    k = append(k, hb[:]...)
    if tomb {
        k = append(k, 0x01)
    } else {
        k = append(k, 0x00)
    }
    return k
}

// Test key encoding functions
func Test_KeyEncoding(t *testing.T) {
    t.Run("LSS_Key_Encoding", func(t *testing.T) {
        userKey := []byte("mykey")
        lssKey := keyLSS(userKey)
        
        expected := append([]byte("s/"), userKey...)
        require.Equal(t, expected, lssKey)
    })
    
    t.Run("HSS_Key_Encoding", func(t *testing.T) {
        userKey := []byte("mykey")
        height := uint64(12345)
        hssKey := keyHSS(height, userKey)
        
        require.Equal(t, byte('h'), hssKey[0])
        require.Equal(t, byte('/'), hssKey[1])
        require.Equal(t, byte('/'), hssKey[10]) // after 8-byte height
        require.Equal(t, userKey, hssKey[11:])
    })
    
    t.Run("Height_Bounds", func(t *testing.T) {
        height := uint64(100)
        lb, ub := boundsForHeight(height)
        
        // Lower bound: h/<height>/
        require.Equal(t, byte('h'), lb[0])
        require.Equal(t, byte('/'), lb[1])
        require.Equal(t, byte('/'), lb[10])
        
        // Upper bound: h/<height+1>/ (exclusive)
        require.NotNil(t, ub)
        require.Equal(t, byte('h'), ub[0])
        require.Equal(t, byte('/'), ub[1])
        require.Equal(t, byte('/'), ub[10])
        // ub represents height+1, so we don't check for 0xFF anymore
    })
}

// Test garbage collection simulation
func Test_GarbageCollection(t *testing.T) {
    db, err := pebble.Open("", &pebble.Options{FS: vfs.NewMem()}) // Enable WAL for DeleteRange
    require.NoError(t, err)
    defer db.Close()

    // Write data across multiple heights using batch
    key := []byte("gctest")
    batch := db.NewBatch()
    for h := uint64(1); h <= 100; h++ {
        hssKey := keyHSS(h, key)
        value := []byte(fmt.Sprintf("value_at_height_%d", h))
        require.NoError(t, batch.Set(hssKey, value, nil))
    }
    require.NoError(t, batch.Commit(pebble.Sync))

    // Simulate GC: retain last 50 heights, delete heights 1-50
    retention := uint64(50)
    currentHeight := uint64(100)
    cutoffHeight := currentHeight - retention

    lb, ub := boundsForHeightRange(1, cutoffHeight)
    require.NoError(t, db.DeleteRange(lb, ub, pebble.Sync))

    // Verify old data is gone
    oldKey := keyHSS(25, key)
    _, _, err = db.Get(oldKey)
    require.Equal(t, pebble.ErrNotFound, err)

    // Verify recent data still exists
    recentKey := keyHSS(75, key)
    val, closer, err := db.Get(recentKey)
    require.NoError(t, err)
    defer closer.Close()
    require.Equal(t, []byte("value_at_height_75"), val)

    t.Logf("✓ Garbage collection working correctly")
}
