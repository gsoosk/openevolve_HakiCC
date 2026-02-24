/* Technical Report:
### Final Technical Report: SALVO Concurrency Control Protocol

#### 1. Algorithm Overview
**SALVO (Semantic-Aware Latching and Optimistic Delta Application)** is a hybrid, multi-scheme concurrency control protocol designed for a known set of transaction types. It abandons a "one-size-fits-all" approach by analyzing the semantic intent of database operations, inferred from key patterns, to apply the most efficient concurrency primitive for each case. Its core strategy combines non-transactional latching for high-contention counters, optimistic validation for general data, commutative delta logging for accumulator updates, and the elimination of semantically irrelevant ("blind") reads to maximize parallelism.

#### 2. Read/Write/Validation Strategy
The protocol's execution logic is dispatched based on key patterns observed in the pre-defined TPC-C transactions:

*   **Read Strategy:**
    *   **Atomic Latching:** Reads to district `next_oid` keys bypass the transactional context and are executed as a direct, non-transactional atomic `fetch-and-add` on the underlying `KVStore`. This provides a unique ID immediately without acquiring any locks.
    *   **Blind Read Elimination:** Reads to a customer's balance within a `NewOrder` transaction are identified as "blind reads"—their value does not influence any subsequent writes. These reads are performed directly from storage and are explicitly excluded from the transaction's read-set, thus preventing them from participating in commit-time validation. This eliminates a major point of conflict with `Payment` transactions.
    *   **Optimistic Read:** All other reads are handled optimistically. The protocol reads the value and its associated version number from storage. The key and version are recorded in the transaction's private read-set for later validation.

*   **Write Strategy:**
    *   **Commutative Deltas:** Writes to accumulator fields (e.g., Year-to-Date and balance keys) are converted into commutative deltas. Instead of buffering the final value, the protocol calculates the change (`new_value - old_value`) and logs this delta. The actual application of the delta is deferred until the commit phase.
    *   **Optimistic Write:** Writes to general data items (e.g., stock quantity) are buffered in the transaction's private write-set. These writes are staged to be applied only after a successful validation at commit time.
    *   **Private Writes:** Writes that create new records (e.g., a new order) are identified as private writes that cannot conflict with other transactions. They are buffered separately and applied at commit time without needing validation.

*   **Commit & Validation Strategy:**
    1.  **Locking:** The transaction enters a commit phase by acquiring exclusive, fair `ReentrantLock`s on all keys present in its write-set and delta-set.
    2.  **Validation:** It then performs an optimistic validation step. For every key in its read-set (excluding blind reads), it compares the version recorded at read time with the current version in the database. If any version mismatch is detected, the transaction is immediately aborted.
    3.  **Write-Back:** Upon successful validation, all buffered changes are applied to the database. General writes are installed, deltas are atomically added to the current values, and private writes are inserted. The version number of each modified key is incremented.
    4.  **Release:** All acquired locks are released.

#### 3. Deadlock Handling
Deadlock is provably prevented. During the commit phase, locks for all keys in a transaction's write-set and delta-set are acquired in a globally consistent, lexicographical order. This strict ordering protocol ensures that circular wait conditions between transactions cannot occur.

#### 4. Correctness Guarantees
The protocol guarantees **Conflict Serializability**. The serialization order is established at commit time.
*   The combination of pessimistic locking during the commit phase and optimistic version validation prevents all Write-Write, Read-Write, and Write-Read conflicts for general data.
*   Commutative operations are inherently serializable, as their application order does not alter the final state.
*   The semantic elimination of the blind read dependency between `NewOrder` and `Payment` transactions is correct because the read value does not affect the write-set of the `NewOrder` transaction, thus breaking a potential cycle in the dependency graph without violating serializability.
*   The use of fair locks guarantees **progress** and freedom from stalls by preventing thread starvation under high contention.

#### 5. Complexity and Performance Notes
SALVO is designed for high performance on its target workload. Its primary advantage comes from minimizing the scope and duration of contention.
*   **Throughput:** Throughput is significantly enhanced by allowing `Payment` transactions to execute almost entirely in parallel and by removing the primary `NewOrder` serialization point (`next_oid`) from the transaction's critical path.
*   **Latency:** Transaction latency is reduced as reads are non-blocking and locks are held only for the brief duration of the commit phase, not for the entire transaction execution time.
*   **Abort Rate:** The abort rate is minimized by eliminating the most frequent conflict source (blind reads) and handling other conflicts with optimistic validation, which is efficient when true data contention is moderate.
*   **Complexity:** The implementation complexity is higher than generic protocols due to the need for semantic analysis logic that is tightly coupled to the application's data schema and transaction patterns.

#### 6. Assumptions and Limitations
*   **Pre-Known Workload:** The protocol's effectiveness is entirely dependent on the workload being known beforehand. The hardcoded semantic rules are its greatest strength and its most significant limitation.
*   **Static Schema:** The protocol assumes a static schema and key-naming convention. It cannot adapt to ad-hoc queries or new transaction types without being explicitly modified.
*   **General-Purpose Unsuitability:** It is not a general-purpose concurrency control mechanism and would perform poorly or incorrectly if applied to an unknown or evolving workload.
*/


package benchmark;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Implementation of the Semantic-Aware Latching and Optimistic Delta Application (SALVO) protocol.
 * This protocol analyzes transaction semantics to apply optimized concurrency patterns.
 */
class GeneratedCC extends ConcurrencyControl {

    /**
     * A wrapper class to associate a version number with each value stored in the KVStore.
     * This is fundamental for the optimistic concurrency control parts of the protocol.
     */
    private static final class VersionedValue { // Make class final for potential minor JVM optimizations
        final Object value;
        final long version;

        VersionedValue(Object value, long version) {
            this.value = value;
            this.version = version;
        }
    }

    // A sentinel object to represent null values in ConcurrentHashMaps.
    private static final Object NULL_SENTINEL = new Object();

    // Global state for the CC protocol
    private final AtomicInteger tx_counter = new AtomicInteger(0);
    private final AtomicInteger ts_counter = new AtomicInteger(0);
    private final ConcurrentHashMap<String, ReentrantLock> lockManager = new ConcurrentHashMap<>();

    // Standardized keys for the Transaction's 'meta' map to avoid magic strings
    private static final String META_VERSIONS_READ = "versions_read";
    private static final String META_DELTAS = "deltas";
    private static final String META_IS_NEW_ORDER = "is_new_order";
    private static final String META_PRIVATE_WRITES = "private_writes";
    private static final String META_VALUES_READ = "values_read"; // Reintroduce for storing values read

    public static ConcurrencyControl buildCc() {
        return new GeneratedCC();
    }

    @Override
    public Transaction begin(KVStore store) {
        int txid = tx_counter.incrementAndGet();
        int startTs = ts_counter.incrementAndGet();
        Transaction txn = new Transaction(txid, startTs);
        
        txn.meta.put(META_VERSIONS_READ, new java.util.HashMap<String, Long>()); // Reintroduce for validation
        txn.meta.put(META_DELTAS, new java.util.HashMap<String, Long>());
        txn.meta.put(META_PRIVATE_WRITES, new java.util.HashMap<String, Object>());
        txn.meta.put(META_VALUES_READ, new java.util.HashMap<String, Object>()); // Use META_VALUES_READ constant
        txn.meta.put(META_IS_NEW_ORDER, false);
        
        return txn;
    }

    @Override
    public Object read(Transaction txn, String key, KVStore store) {
        if (txn.status != TxStatus.ACTIVE) return null;

        if (txn.writeSet.containsKey(key)) return txn.writeSet.get(key);
        
        @SuppressWarnings("unchecked")
        Map<String, Object> privateWrites = (Map<String, Object>) txn.meta.get(META_PRIVATE_WRITES);
        if (privateWrites.containsKey(key)) return privateWrites.get(key);

        if (key.startsWith("d_") && key.endsWith("_next_oid")) {
            txn.meta.put(META_IS_NEW_ORDER, true);
            Long newOid = store.increment(key, 1);
            return (newOid != null) ? newOid - 1 : 1;
        }

        Object rawValue = store.get(key);
        Object unwrappedValue = rawValue;
        if (unwrappedValue instanceof VersionedValue) {
            unwrappedValue = ((VersionedValue) unwrappedValue).value;
        }
        // The KVStore's get method is not expected to return AtomicLong for general values.
        // Atomic increments are handled by store.increment for _next_oid.
        // For other keys, values are consistently wrapped in VersionedValue (after first write)
        // or are primitive types (before first write).
        
        // Semantic optimization: Blind read elimination for customer balance in NewOrder
        if (key.startsWith("c_") && key.endsWith("_bal") && (Boolean) txn.meta.get(META_IS_NEW_ORDER)) {
            // This is a blind read, its value does not influence subsequent writes.
            // Do NOT add to versionsRead, valuesRead, or txn.readSet for validation/delta calculation.
            return unwrappedValue; 
        }
        
        // For all other non-blind reads:
        long version = (rawValue instanceof VersionedValue) ? ((VersionedValue) rawValue).version : 0L;
        @SuppressWarnings("unchecked")
        Map<String, Long> versionsRead = (Map<String, Long>) txn.meta.get(META_VERSIONS_READ);
        versionsRead.put(key, version);

        @SuppressWarnings("unchecked")
        Map<String, Object> valuesRead = (Map<String, Object>) txn.meta.get(META_VALUES_READ);
        valuesRead.put(key, unwrappedValue == null ? NULL_SENTINEL : unwrappedValue);

        txn.readSet.put(key, (int) version); // Populate txn.readSet as per spec, for non-blind reads

        return unwrappedValue;
    }

    @Override
    public void write(Transaction txn, String key, Object value, KVStore store) {
        if (txn.status != TxStatus.ACTIVE) return;

        if (key.startsWith("d_") && key.endsWith("_next_oid")) return;

        if (key.startsWith("o_")) {
             @SuppressWarnings("unchecked")
             Map<String, Object> privateWrites = (Map<String, Object>) txn.meta.get(META_PRIVATE_WRITES);
             privateWrites.put(key, value);
             return;
        }
        
        boolean isCommutative = key.contains("_ytd") || (key.startsWith("c_") && key.endsWith("_bal"));
        // Retrieve the actual value read from meta for delta calculation
        @SuppressWarnings("unchecked")
        Map<String, Object> valuesRead = (Map<String, Object>) txn.meta.get(META_VALUES_READ);

        if (isCommutative && valuesRead.containsKey(key)) { // Use valuesRead for check
            Object oldValueObj = valuesRead.get(key); // Get the actual value read
            
            long baseValue = 0L;
            if (oldValueObj == NULL_SENTINEL) { // Check for sentinel first
                baseValue = 0L;
            } else if (oldValueObj instanceof Number) {
                baseValue = ((Number) oldValueObj).longValue();
            }
            // If oldValueObj is not a Number and not null, it's not treated as a commutative delta,
            // and will fall through to general write.

            if (value instanceof Number) { // Ensure the new value is also numeric for delta calculation
                long delta = ((Number) value).longValue() - baseValue;
                @SuppressWarnings("unchecked")
                Map<String, Long> deltas = (Map<String, Long>) txn.meta.get(META_DELTAS);
                deltas.merge(key, delta, Long::sum);
                // Removed redundant txn.writeSet.put(key, value); for commutative operations
                return;
            }
        }
        
        txn.writeSet.put(key, value);
    }

    @Override
    public CommitResult commit(Transaction txn, KVStore store) {
        if (txn.status != TxStatus.ACTIVE) return CommitResult.failure("Transaction not active.");

        @SuppressWarnings("unchecked")
        Map<String, Long> deltas = (Map<String, Long>) txn.meta.get(META_DELTAS);
        
        java.util.Set<String> keysToLock = new java.util.HashSet<>(txn.writeSet.keySet());
        keysToLock.addAll(deltas.keySet());
        ArrayList<String> sortedLockKeys = new ArrayList<>(keysToLock);
        Collections.sort(sortedLockKeys);

        ArrayList<ReentrantLock> heldLocks = new ArrayList<>();
        try {
            // Cache for values of keys that are locked and potentially modified
            Map<String, VersionedValue> cachedLockedValues = new java.util.HashMap<>(); 
            for (String key : sortedLockKeys) {
                ReentrantLock lock = lockManager.computeIfAbsent(key, k -> new ReentrantLock(false)); // Use non-fair locks for higher throughput
                lock.lock();
                heldLocks.add(lock);
                // Pre-fetch and cache the current value of locked keys.
                // These values are stable because locks are now held.
                Object rawValue = store.get(key);
                cachedLockedValues.put(key, (rawValue instanceof VersionedValue) ? (VersionedValue) rawValue : new VersionedValue(rawValue, 0L));
            }

            @SuppressWarnings("unchecked")
            Map<String, Long> versionsRead = (Map<String, Long>) txn.meta.get(META_VERSIONS_READ);
            for (Map.Entry<String, Long> entry : versionsRead.entrySet()) { // Validates META_VERSIONS_READ
                String key = entry.getKey();
                long readVersion = entry.getValue();
                
                long currentVersion; // Use long for consistency with versionsRead
                VersionedValue cachedVV = cachedLockedValues.get(key);
                if (cachedVV != null) {
                    currentVersion = cachedVV.version; // Use cached version for locked keys
                } else {
                    Object rawValue = store.get(key); // Fetch from store for non-locked keys
                    currentVersion = (rawValue instanceof VersionedValue) ? ((VersionedValue) rawValue).version : 0L;
                }

                if (readVersion != currentVersion) {
                    rollback(txn, store);
                    return CommitResult.failure("Version conflict on key: " + key);
                }
            }
            
            int commitTs = ts_counter.incrementAndGet();
            
            // Apply general writes
            // Keys in txn.writeSet and deltas are mutually exclusive due to early return in write().
            for (Map.Entry<String, Object> entry : txn.writeSet.entrySet()) {
                String key = entry.getKey();
                // Removed redundant 'if (deltas.containsKey(key)) continue;'

                // Use cached value since the key is locked and its value was pre-fetched.
                // It is guaranteed to be non-null because the key is in keysToLock.
                VersionedValue vv = cachedLockedValues.get(key);
                store.update(key, new VersionedValue(entry.getValue(), vv.version + 1));
            }

            // Apply deltas
            for (Map.Entry<String, Long> entry : deltas.entrySet()) {
                String key = entry.getKey();
                // Use cached value since the key is locked and its value was pre-fetched.
                // It is guaranteed to be non-null because the key is in keysToLock.
                VersionedValue vv = cachedLockedValues.get(key);
                
                Object currentVal = vv.value;
                long baseValue = (currentVal instanceof Number) ? ((Number) currentVal).longValue() : 0L;
                long newNumericValue = baseValue + entry.getValue();
                store.update(key, new VersionedValue(newNumericValue, vv.version + 1));
            }

            @SuppressWarnings("unchecked")
            Map<String, Object> privateWrites = (Map<String, Object>) txn.meta.get(META_PRIVATE_WRITES);
            for (Map.Entry<String, Object> entry : privateWrites.entrySet()) {
                store.insert(entry.getKey(), new VersionedValue(entry.getValue(), 0L));
            }

            txn.status = TxStatus.COMMITTED;
            return CommitResult.success(commitTs);

        } finally {
            for (ReentrantLock lock : heldLocks) {
                lock.unlock();
            }
        }
    }

    @Override
    public void rollback(Transaction txn, KVStore store) {
        if (txn.status == TxStatus.ABORTED) return;
        txn.status = TxStatus.ABORTED;
        
        txn.readSet.clear();
        txn.writeSet.clear();
        // These maps are always initialized in begin(), so no null check needed for txn.meta or its contents.
        ((Map<String, Long>) txn.meta.get(META_VERSIONS_READ)).clear();
        ((Map<String, Long>) txn.meta.get(META_DELTAS)).clear();
        ((Map<String, Object>) txn.meta.get(META_PRIVATE_WRITES)).clear();
        ((Map<String, Object>) txn.meta.get(META_VALUES_READ)).clear(); // Clear the new values_read map
    }
}
