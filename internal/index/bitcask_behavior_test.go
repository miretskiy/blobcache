package index

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.mills.io/bitcask/v2"
)

// TestBitcask_TransactionIsolation verifies how Bitcask transactions handle
// concurrent writes from outside the transaction.
//
// Critical question: Does a transaction see records written AFTER the transaction started?
//
// This determines our tombstone deletion protocol during compaction.
func TestBitcask_TransactionIsolation(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := bitcask.Open(tmpDir)
	require.NoError(t, err)
	defer db.Close()

	// T0: Write initial value
	require.NoError(t, db.Put([]byte("key1"), []byte("initial")))
	require.NoError(t, db.Put([]byte("key2"), []byte("v2")))

	// T1: Start transaction (snapshot point)
	txn := db.Transaction()
	defer txn.Discard()

	// T2: Transaction reads key1
	val, err := txn.Get([]byte("key1"))
	require.NoError(t, err)
	require.EqualValues(t, "initial", val, "txn should see initial value")

	// T3: External writer updates key1 (AFTER transaction started)
	require.NoError(t, db.Put([]byte("key1"), []byte("updated-outside-txn")))

	// T4: External writer creates new key3 (AFTER transaction started)
	require.NoError(t, db.Put([]byte("key3"), []byte("new-key")))

	// T5: Transaction reads key1 again
	val, err = txn.Get([]byte("key1"))
	require.NoError(t, err)

	// CRITICAL TEST: What does the transaction see?
	if string(val) == "initial" {
		t.Log("✅ SNAPSHOT ISOLATION: Transaction does NOT see external writes")
		t.Log("   This is ideal for compaction - we have a consistent view")
	} else {
		t.Logf("⚠️  NO ISOLATION: Transaction SEES external writes (value: %s)", val)
		t.Log("   This is problematic - we need defensive protocols")
		t.Fatal("Transaction saw external write - need to rethink compaction protocol")
	}

	// T6: Transaction reads key3 (created after txn started)
	val, err = txn.Get([]byte("key3"))

	if err != nil {
		t.Log("✅ SNAPSHOT ISOLATION: Transaction does NOT see new keys")
	} else {
		t.Logf("⚠️  NO ISOLATION: Transaction sees new key: %s", val)
		t.Fatal("Transaction saw new key - need to rethink compaction protocol")
	}

	// T7: Transaction writes to key1 and commits
	require.NoError(t, txn.Put([]byte("key1"), []byte("txn-write")))
	require.NoError(t, txn.Commit())

	// T8: Verify final state after commit
	val, err = db.Get([]byte("key1"))
	require.NoError(t, err)

	t.Logf("Final value after txn commit: %s", val)
	if string(val) == "txn-write" {
		t.Log("✅ LAST WRITE WINS: Transaction commit overwrote external write")
	} else {
		t.Logf("⚠️  EXTERNAL WRITE WINS: Transaction commit was ignored (value: %s)", val)
	}
}

// TestBitcask_TransactionRange verifies if Range() sees concurrent writes.
func TestBitcask_TransactionRange(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := bitcask.Open(tmpDir)
	require.NoError(t, err)
	defer db.Close()

	// T0: Write initial keys
	require.NoError(t, db.Put([]byte("seg:001:0"), []byte("data1")))
	require.NoError(t, db.Put([]byte("seg:001:1"), []byte("data2")))

	// T1: Start transaction
	txn := db.Transaction()
	defer txn.Discard()

	// T2: Count keys in range before external write
	countBefore := 0
	err = txn.Range([]byte("seg:001:"), []byte("seg:002:"), func(key bitcask.Key) error {
		countBefore++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 2, countBefore)

	// T3: External write adds new key in range
	require.NoError(t, db.Put([]byte("seg:001:2"), []byte("data3")))

	// T4: Count keys in range after external write
	countAfter := 0
	err = txn.Range([]byte("seg:001:"), []byte("seg:002:"), func(key bitcask.Key) error {
		countAfter++
		return nil
	})
	require.NoError(t, err)

	if countAfter == 2 {
		t.Log("✅ SNAPSHOT ISOLATION: Range does NOT see external writes")
	} else if countAfter == 3 {
		t.Log("⚠️  NO ISOLATION: Range SEES external writes")
		t.Fatal("Transaction Range saw external write - compaction unsafe")
	}
}

// TestBitcask_ConcurrentDeleteDuringTransaction tests deletion visibility.
func TestBitcask_ConcurrentDeleteDuringTransaction(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := bitcask.Open(tmpDir)
	require.NoError(t, err)
	defer db.Close()

	// T0: Write key
	require.NoError(t, db.Put([]byte("victim"), []byte("data")))

	// T1: Start transaction
	txn := db.Transaction()
	defer txn.Discard()

	// T2: Transaction sees key
	val, err := txn.Get([]byte("victim"))
	require.NoError(t, err)
	require.EqualValues(t, "data", val)

	// T3: External delete
	require.NoError(t, db.Delete([]byte("victim")))

	// T4: Transaction reads again
	val, err = txn.Get([]byte("victim"))

	if err != nil {
		t.Log("⚠️  SEES DELETION: Transaction sees external delete")
		t.Fatal("Transaction saw external delete - problematic for compaction")
	} else {
		t.Log("✅ SNAPSHOT ISOLATION: Transaction still sees deleted key")
	}
}
