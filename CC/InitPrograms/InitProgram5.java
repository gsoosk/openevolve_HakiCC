package benchmark;

/* Technical Report:
### **Technical Report: Optimistic Concurrency Control with Atomic Snapshot Validation**

*   **Algorithm Overview:** The protocol is a strict, optimistic concurrency control (OCC) algorithm that uses versioning to ensure serializability. Transactions execute in three phases: an optimistic execution phase where reads and writes are buffered locally, a validation phase, and a write phase. The protocol's key feature is an atomic intra-read validation mechanism that guarantees a transaction always operates on a consistent snapshot of the database, preventing anomalies like write skew.

*   **Read/Write Strategy:**
    *   **Read Operation:** When a transaction reads a key for the first time, it performs an atomic validation of its existing read-set. This is achieved by acquiring a global lock, re-checking the versions of all previously read keys, and aborting if any have changed. If the snapshot is valid, the lock is released, and the transaction reads the new key's value and its current version from the store. Subsequent reads of the same key return the locally cached value.
    *   **Write Operation:** Writes are buffered in the transaction's private write-set. To handle "blind writes" (writing to a key not previously read), the write operation first invokes the read operation on the key. This ensures every key in the write-set has a corresponding version dependency established for validation.

*   **Validation Strategy:** Validation occurs at two points: proactively during reads (as described above) and finally at commit time. The commit phase is executed atomically under a single global lock. The transaction re-validates the versions of all keys in its read-set against the global versions. If all versions match, the transaction is successful. If any version mismatch is found, the transaction is immediately aborted.

*   **Deadlock Handling:** The protocol is **deadlock-free**. It does not use per-data-item locks during transaction execution. The use of a single global lock for short, well-defined critical sections (snapshot validation and commit) prevents any possibility of circular wait conditions.

*   **Correctness Guarantees:** The protocol ensures **Strict Conflict-Serializability**.
    *   **Conflict-Serializability:** The combination of atomic snapshot validation during reads and a final atomic validation at commit time guarantees that any transaction operates on a consistent view of the database. This prevents read-write and write-write conflicts from producing non-serializable histories.
    *   **Strictness:** All writes are buffered and applied to the database atomically only after the transaction has successfully passed validation. This ensures that uncommitted data is never visible, preventing dirty reads and cascading aborts.

*   **Complexity and Performance Notes:**
    *   The primary performance trade-off is the use of a global lock to validate the snapshot during read operations. This serializes a portion of the read path, adding latency that scales with the size of a transaction's read-set.
    *   The benefit of this approach is a significant reduction in wasted work, as transactions operating on an invalidated snapshot are aborted early rather than continuing to execute only to fail at commit time.
    *   The single commit lock can be a bottleneck under very high commit rates, limiting scalability on systems with a large number of cores. The protocol is best suited for workloads with moderate to high contention where transaction durations are relatively short.

*   **Assumptions and Limitations:**
    *   The protocol assumes a single-node database architecture.
    *   A key limitation is the global lock, which impacts the scalability of read operations. This makes the protocol less suitable for long-running, read-intensive analytical transactions.
    *   The protocol is optimized for correctness and robustness, trading some potential throughput for guaranteed serializability in complex transactional scenarios.
*/


// EVOLVE-BLOCK-START
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Implementation of a correct, strict Optimistic Concurrency Control (OCC) protocol.
 *
 * This version fixes serializability violations by ensuring snapshot consistency.
 * It uses an atomic intra-read validation mechanism. Before a transaction reads a
 * new key, it acquires a global lock to atomically verify that all previously
 * read keys are still valid. This prevents the transaction from operating on an
 * inconsistent snapshot of the database, thus avoiding anomalies like write skew.
 */
class GeneratedCC extends ConcurrencyControl {

    private final AtomicInteger transactionCounter = new AtomicInteger(0);
    private final ConcurrentHashMap<String, AtomicInteger> versionMap = new ConcurrentHashMap<>();
    private final ReentrantLock commitLock = new ReentrantLock();

    private static final String READ_VERSIONS_KEY = "readVersions";
    private static final Object NULL_SENTINEL = new Object();

    public static ConcurrencyControl buildCc() {
        return new GeneratedCC();
    }

    @Override
    public Transaction begin(KVStore store) {
        int txid = transactionCounter.incrementAndGet();
        Transaction txn = new Transaction(txid, txid);
        txn.meta.put(READ_VERSIONS_KEY, new ConcurrentHashMap<String, Integer>());
        return txn;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object read(Transaction txn, String key, KVStore store) {
        if (txn.status != TxStatus.ACTIVE) return null;
        if (txn.writeSet.containsKey(key)) return txn.writeSet.get(key);
        
        if (txn.readSet.containsKey(key)) {
            Object value = txn.readSet.get(key);
            return value == NULL_SENTINEL ? null : value;
        }

        Map<String, Integer> readVersions = (Map<String, Integer>) txn.meta.get(READ_VERSIONS_KEY);

        // --- ATOMIC INTRA-TRANSACTION SNAPSHOT VALIDATION ---
        // Before reading a new key, atomically ensure the existing snapshot is still valid.
        // This prevents the transaction from building an inconsistent view of the data.
        commitLock.lock();
        try {
            for (Map.Entry<String, Integer> entry : readVersions.entrySet()) {
                String validatedKey = entry.getKey();
                int recordedVersion = entry.getValue();
                
                int currentVersion = versionMap.computeIfAbsent(validatedKey, k -> new AtomicInteger(0)).get();
                if (recordedVersion != currentVersion) {
                    // Snapshot is broken by a concurrent commit. Abort immediately.
                    rollback(txn, store);
                    return null;
                }
            }
        } finally {
            commitLock.unlock();
        }

        // If validation caused an abort, the status will have changed.
        if (txn.status != TxStatus.ACTIVE) return null;

        // --- READ NEW KEY (if validation passed) ---
        Object valueFromStore;
        int versionRead;
        while (true) {
            versionRead = versionMap.computeIfAbsent(key, k -> new AtomicInteger(0)).get();
            valueFromStore = store.get(key);
            int versionAfterRead = versionMap.get(key).get();
            if (versionRead == versionAfterRead) break;
        }

        Object returnValue = (valueFromStore instanceof AtomicLong) ? ((AtomicLong) valueFromStore).get() : valueFromStore;
        
        readVersions.put(key, versionRead);
        txn.readSet.put(key, returnValue == null ? NULL_SENTINEL : returnValue);

        return returnValue;
    }

    @Override
    public void write(Transaction txn, String key, Object value, KVStore store) {
        if (txn.status != TxStatus.ACTIVE) return;

        // Eagerly establish a version dependency for blind writes.
        if (!txn.readSet.containsKey(key) && !txn.writeSet.containsKey(key)) {
            read(txn, key, store);
        }

        // The read() call might have aborted the transaction. Check status before proceeding.
        if (txn.status != TxStatus.ACTIVE) return;

        txn.writeSet.put(key, value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public CommitResult commit(Transaction txn, KVStore store) {
        if (txn.status != TxStatus.ACTIVE) {
            return CommitResult.failure("Transaction already " + txn.status);
        }

        Map<String, Integer> readVersions = (Map<String, Integer>) txn.meta.get(READ_VERSIONS_KEY);
        
        commitLock.lock();
        try {
            // Final validation at commit time is still required.
            for (Map.Entry<String, Integer> entry : readVersions.entrySet()) {
                String key = entry.getKey();
                int readVersion = entry.getValue();
                
                int currentVersion = versionMap.computeIfAbsent(key, k -> new AtomicInteger(0)).get();

                if (readVersion != currentVersion) {
                    rollback(txn, store);
                    return CommitResult.failure("Validation failed on key: " + key);
                }
            }

            // --- Write Phase ---
            int commitTs = transactionCounter.incrementAndGet();
            
            for (Map.Entry<String, Object> entry : txn.writeSet.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();

                store.update(key, value);
                versionMap.get(key).incrementAndGet();
            }

            txn.status = TxStatus.COMMITTED;
            return CommitResult.success(commitTs);

        } finally {
            commitLock.unlock();
        }
    }

    @Override
    public void rollback(Transaction txn, KVStore store) {
        if (txn.status == TxStatus.ACTIVE) {
            txn.status = TxStatus.ABORTED;
            txn.readSet.clear();
            txn.writeSet.clear();
            txn.meta.clear();
        }
    }
}
// EVOLVE-BLOCK-END