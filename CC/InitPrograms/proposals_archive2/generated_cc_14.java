/* Technical Report:
### Final Report: Lock-Protected Optimistic Concurrency Control (LP-OCC)

#### 1. Algorithm Overview
This protocol is a robust implementation of Optimistic Concurrency Control (OCC) that prioritizes correctness and serializability by eliminating race conditions found in more complex, lock-free designs. The core principle is "read-validate-write," but with the critical addition of locking to protect the atomicity of both the read and commit phases. Transactions execute optimistically, buffering their reads and writes in a private workspace. At commit time, a validation phase ensures that no data read by the transaction has been modified by other concurrently committing transactions. The protocol's main innovation lies in its use of short-term locks during the read phase to guarantee a consistent snapshot, thereby preventing subtle race conditions that can undermine the validation logic.

#### 2. Read/Write/Lock and Validation Strategy
*   **Read Operation:** To ensure a transaction reads a consistent snapshot of a value and its associated version number, a short-term lock is acquired on the key being read. While holding the lock, the value is read from the underlying store, and its version is read from the protocol's version manager. Both are stored in the transaction's private workspace. The lock is released immediately afterward. This lock-protected read prevents reader/writer race conditions where a transaction might observe a partially committed write from another transaction.
*   **Write Operation:** All writes are buffered in the transaction's private `writeSet`. A "write-implies-read" rule is enforced: if a transaction's first access to a key is a write, it must first perform a consistent, lock-protected read (as described above) to establish a version dependency for that key. This ensures all written keys are validated at commit time, preventing lost updates.
*   **Validation and Commit Strategy:** The commit phase follows a strict "lock-validate-write" sequence.
    1.  **Locking:** The transaction acquires locks for the union of all keys in its read and write sets. Locks are acquired in a canonical (lexicographical) order to prevent deadlocks.
    2.  **Validation:** While holding the locks, the transaction iterates through its read set, comparing the version number it recorded for each key against the current version in the global version manager. If any version has changed, the transaction aborts, and its changes are discarded.
    3.  **Writing:** If validation succeeds, the transaction applies all buffered writes to the data store and increments the version number for each modified key.
    4.  **Unlocking:** All locks are released, making the changes visible to other transactions.

#### 3. Deadlock Handling
The protocol primarily relies on **deadlock prevention**. During the commit phase, all required locks are acquired in a globally consistent canonical order (lexicographical by key name). This prevents the circular-wait condition necessary for a deadlock to occur among committing transactions. Since locks acquired during the read phase are short-lived and do not overlap with other long-term locks, they do not introduce a risk of deadlock.

#### 4. Correctness Guarantees
The protocol guarantees **Strict Conflict-Serializability**.
*   **Conflict-Serializability:** This is ensured by the atomic commit phase. By locking the entire read and write set before validation, the transaction guarantees that its snapshot is valid at the moment of validation and that no other transaction can interfere between validation and writing. This effectively establishes a single, total serialization point for all transactions at their commit time.
*   **Strictness:** The protocol is strict because writes are buffered and only applied to the database after a successful commit. This ensures that no transaction can read the uncommitted writes of another transaction, thus avoiding cascading aborts.

#### 5. Complexity and Performance Notes
The main performance trade-off of this protocol is sacrificing lock-free reads for guaranteed correctness. Acquiring a lock for every initial read introduces overhead and potential contention compared to purely optimistic or seqlock-based approaches. However, it completely eliminates a class of subtle race conditions that are difficult to debug and can lead to data corruption. The performance is sensitive to the duration of the commit phase, as locks are held during validation and writing. Its performance is best suited for workloads with low to moderate data contention, where the overhead of read-locking is minimal and the abort rate remains low. Under high contention, it may suffer from increased lock-waiting latency.

#### 6. Assumptions and Limitations
*   The protocol assumes that the overhead of acquiring a short-term lock during the read phase is acceptable. In environments with extremely low-latency read requirements, this could be a bottleneck.
*   It does not include any semantic optimizations (e.g., for commutative operations). All operations are treated uniformly, which simplifies the design but may miss opportunities for higher throughput in specific workloads like TPC-C.
*   The protocol is not deadlock-free by design but relies on a strict locking discipline for deadlock prevention, which is effective but requires careful implementation.Novelty Result:
*/

package benchmark;

// EVOLVE-BLOCK-START


import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Implements a robust Optimistic Concurrency Control (OCC) protocol.
 *
 * This version corrects fundamental serializability bugs by simplifying the design
 * to a classic OCC implementation with lock-protected reads. Previous attempts
 * with complex, lock-free seqlocks and transaction classifications introduced
 * subtle race conditions leading to widespread failures.
 *
 * Key principles of this corrected implementation:
 * 1.  **No Transaction Specialization:** All transactions are treated uniformly.
 *     The complex and buggy commutative/delta logic has been removed.
 * 2.  **Lock-Protected Reads:** To guarantee that a transaction reads a
 *     consistent snapshot of a value and its version, `performConsistentRead`
 *     briefly acquires a lock on the key. This serializes the read against any
 *     concurrent commit writing to the same key, eliminating reader/writer races.
 * 3.  **Standard OCC Commit Phase:** The commit logic follows the standard
 *     "lock-validate-write-unlock" pattern. It acquires locks for all keys in
 *     the read and write sets to create a critical section, validates read
 *     versions, applies writes, and releases the locks.
 */
class GeneratedCC extends ConcurrencyControl {

    private static final Object NULL_SENTINEL = new Object();
    private final ConcurrentHashMap<String, ReentrantLock> lockManager = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicInteger> versionManager = new ConcurrentHashMap<>();
    private final AtomicInteger transactionIdCounter = new AtomicInteger(0);
    private final AtomicInteger globalTimestamp = new AtomicInteger(0);

    private static final String VERSIONED_READ_SET_KEY = "versioned_read_set";

    public static ConcurrencyControl buildCc() {
        return new GeneratedCC();
    }

    @Override
    public Transaction begin(KVStore store) {
        int txid = transactionIdCounter.incrementAndGet();
        Transaction txn = new Transaction(txid, globalTimestamp.get());
        txn.meta.put(VERSIONED_READ_SET_KEY, new ConcurrentHashMap<String, Integer>());
        return txn;
    }

    @SuppressWarnings("unchecked")
    private Object performConsistentRead(Transaction txn, String key, KVStore store) {
        Map<String, Integer> versionedReadSet = (Map<String, Integer>) txn.meta.get(VERSIONED_READ_SET_KEY);
        ReentrantLock lock = lockManager.computeIfAbsent(key, k -> new ReentrantLock());
        
        lock.lock();
        try {
            Object value = store.get(key);
            int version = versionManager.computeIfAbsent(key, k -> new AtomicInteger(0)).get();

            Object snapshotValue = (value instanceof AtomicLong) ? ((AtomicLong) value).get() : value;
            txn.readSet.put(key, (snapshotValue == null) ? NULL_SENTINEL : snapshotValue);
            versionedReadSet.put(key, version);
            return snapshotValue;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Object read(Transaction txn, String key, KVStore store) {
        if (txn.status != TxStatus.ACTIVE) return null;
        if (txn.writeSet.containsKey(key)) return txn.writeSet.get(key);
        if (txn.readSet.containsKey(key)) {
            Object cachedValue = txn.readSet.get(key);
            return (cachedValue == NULL_SENTINEL) ? null : cachedValue;
        }
        return performConsistentRead(txn, key, store);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void write(Transaction txn, String key, Object value, KVStore store) {
        if (txn.status != TxStatus.ACTIVE) return;

        // "Write implies read": Ensure a version dependency exists before any write.
        Map<String, Integer> versionedReadSet = (Map<String, Integer>) txn.meta.get(VERSIONED_READ_SET_KEY);
        if (!versionedReadSet.containsKey(key)) {
            performConsistentRead(txn, key, store);
        }
        txn.writeSet.put(key, value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public CommitResult commit(Transaction txn, KVStore store) {
        if (txn.status != TxStatus.ACTIVE) {
            return CommitResult.failure("Transaction already " + txn.status);
        }

        Map<String, Integer> versionedReadSet = (Map<String, Integer>) txn.meta.get(VERSIONED_READ_SET_KEY);
        List<String> keysToLock = Stream.concat(versionedReadSet.keySet().stream(), txn.writeSet.keySet().stream())
                .distinct().sorted().collect(Collectors.toList());

        if (keysToLock.isEmpty()) {
            txn.status = TxStatus.COMMITTED;
            return CommitResult.success(globalTimestamp.incrementAndGet());
        }

        List<ReentrantLock> heldLocks = new ArrayList<>();
        try {
            // Phase 1: Lock all keys in canonical order
            for (String key : keysToLock) {
                ReentrantLock lock = lockManager.computeIfAbsent(key, k -> new ReentrantLock());
                lock.lock();
                heldLocks.add(lock);
            }

            // Phase 2: Validate the read set
            for (Map.Entry<String, Integer> entry : versionedReadSet.entrySet()) {
                int readVersion = entry.getValue();
                int currentVersion = versionManager.computeIfAbsent(entry.getKey(), k -> new AtomicInteger(0)).get();
                if (readVersion != currentVersion) {
                    return CommitResult.failure("Validation failed for key: " + entry.getKey());
                }
            }

            // Phase 3: Write to the store
            int commitTs = globalTimestamp.incrementAndGet();
            for (Map.Entry<String, Object> entry : txn.writeSet.entrySet()) {
                store.update(entry.getKey(), entry.getValue());
                versionManager.computeIfAbsent(entry.getKey(), k -> new AtomicInteger(0)).incrementAndGet();
            }

            txn.status = TxStatus.COMMITTED;
            return CommitResult.success(commitTs);
        } finally {
            // Phase 4: Release all locks
            for (ReentrantLock lock : heldLocks) {
                lock.unlock();
            }
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void rollback(Transaction txn, KVStore store) {
        if (txn.status != TxStatus.ACTIVE) return;
        txn.status = TxStatus.ABORTED;
        txn.readSet.clear();
        txn.writeSet.clear();
        ((Map<?, ?>) txn.meta.get(VERSIONED_READ_SET_KEY)).clear();
    }
}
// EVOLVE-BLOCK-END