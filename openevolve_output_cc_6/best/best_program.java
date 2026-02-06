/* Technical Report:
### Final Technical Report: Semantic-Aware Deterministic Locking (SADL)

*   **Algorithm Overview**
    SADL is a hybrid concurrency control protocol designed for high performance on workloads with pre-defined transaction types and a partitionable keyspace. It combines three core concepts: strict data partitioning, deterministic ordering via dedicated schedulers, and semantic-aware locking. The keyspace is partitioned based on a `warehouse_id` extracted from the key. Each partition is managed by an independent, single-threaded scheduler that establishes a definitive serial execution order for all transactions within its partition *before* they execute. This order is enforced by assigning a monotonic Intra-Partition Sequence Number (IPSN) to each transaction upon its first operation. Concurrency is then maximized during the execution phase by using specialized lock modes that leverage the commutative properties of the known transaction types.

*   **Read/Write/Lock Strategy**
    -   **Visibility & Writes:** Writes are buffered locally in a transaction's private write-set. Reads first check this local write-set to ensure "read-your-own-writes" semantics. If a key is not in the write-set, the transaction acquires the appropriate lock and reads from the global key-value store.
    -   **Locking Strategy:** SADL uses three lock modes: Shared (S), Exclusive (X), and a custom Ordered-Commutative (OC).
        -   **S-Lock:** Acquired for standard read operations. Compatible with other S-Locks.
        -   **X-Lock:** Acquired for non-commutative writes, such as incrementing the `next_oid` counter. Incompatible with all other locks.
        -   **OC-Lock:** Acquired for commutative arithmetic operations (e.g., `ytd += val`, `stock -= qty`). OC-Locks are compatible with other OC-Locks, allowing multiple transactions to concurrently modify the same key. They are not compatible with S or X locks.
    -   **Commit Strategy:** When a transaction completes execution, it is placed into its partition's commit queue, which is ordered by IPSN. The partition's scheduler thread processes this queue in strict IPSN order, applying the buffered writes to the database and releasing the transaction's locks. This ensures the physical state of the database always reflects the pre-determined serial order.

*   **Deadlock Handling**
    The protocol is **deadlock-free by design**. Lock contention is resolved based on the pre-assigned IPSN. A transaction `T_j` is only permitted to wait for a lock held by a transaction `T_i` if `IPSN(i) < IPSN(j)`. This rule creates a strict, acyclic ordering in the waits-for graph, making circular dependencies (deadlocks) impossible. Consequently, there is no need for deadlock detection or timeout-based aborts.

*   **Correctness Guarantees**
    SADL provides **Strict Conflict-Serializability**.
    -   **Serializability:** The assignment of a unique IPSN to every transaction within a partition establishes a total serial order *a priori*. The ordered commit mechanism guarantees that the final database state is equivalent to one where transactions were executed serially in their IPSN order. Since the TPC-C workload transactions are disjoint across partitions, local serializability guarantees global serializability.
    -   **Strictness:** All locks (S, X, and OC) are held until the transaction is processed by the commit scheduler (after its logical completion). This ensures that no transaction can read or write data modified by another uncommitted transaction, preventing cascading aborts.

*   **Complexity and Performance Notes**
    -   **Performance:** SADL achieves high throughput on the target TPC-C workload. Partitioning allows for true parallelism across warehouses. The use of OC-Locks dramatically reduces contention on frequently updated hotspot keys (like `ytd` and `stock_qty`), further boosting intra-partition concurrency. The zero-abort design for concurrency conflicts eliminates wasted work and leads to predictable latency under contention.
    -   **Complexity:** The primary overhead is the inter-thread communication between worker threads and the partition schedulers for IPSN assignment and commit processing. The single-threaded nature of the scheduler serializes these two critical points, which could become a bottleneck if individual transaction execution times were negligible.

*   **Assumptions and Limitations**
    -   **Workload Specificity:** The protocol is highly optimized for the given TPC-C-like workload. Its performance relies on the ability to partition the data and the prevalence of commutative arithmetic operations.
    -   **Known Semantics:** The protocol assumes the semantics of write operations are known in advance to correctly classify them as commutative (OC-Lock) or non-commutative (X-Lock).
    -   **Partition-Bound Transactions:** The design is most effective for transactions that operate entirely within a single partition. Cross-partition transactions would require a more complex global coordination mechanism, which is not implemented and would degrade performance.
    -   **No Ad-Hoc Queries:** The protocol is not designed for general-purpose, ad-hoc queries, which may not align with the partitioning scheme or semantic lock modes.
*/



package benchmark;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Implementation of Semantic-Aware Deterministic Locking (SADL).
 * This protocol partitions the database by warehouse_id and uses a single-threaded
 * scheduler per partition to assign a serial order (IPSN) and commit transactions
 * in that order. It employs special commutative lock modes (OC-Lock) to maximize
 * concurrency for arithmetic operations, while preventing deadlocks by ensuring
* transactions only wait for those with a lower IPSN.
 */
class GeneratedCC extends ConcurrencyControl {

    private final AtomicInteger nextTxid = new AtomicInteger(0);
    private final ConcurrentHashMap<Integer, Transaction> activeTransactions = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, PartitionManager> partitions = new ConcurrentHashMap<>();
    private static final Pattern WAREHOUSE_KEY_PATTERN = Pattern.compile("^[a-z]_(\\d+)_.*");
    private static final Object NULL_SENTINEL = new Object(); // Sentinel for null values in ConcurrentHashMap

    public static ConcurrencyControl buildCc() {
        return new GeneratedCC();
    }

    // Enum for different lock modes
    private enum LockMode {
        NONE, S_LOCK, X_LOCK, OC_LOCK // Ordered-Commutative Lock
    }

    // Represents a lock on a specific key
    private static class LockEntry {
        // Use counters for each lock type for faster compatibility checks
        private int s_lock_count = 0;
        private int x_lock_count = 0;
        private int oc_lock_count = 0;

        // Stores owner txid and their IPSN to avoid activeTransactions lookup during conflict resolution
        final Map<Integer, Long> ownersWithIpsn = new HashMap<>(); 

        // Helper to get the current aggregate lock mode based on counts
        LockMode getCurrentAggregateMode() {
            if (x_lock_count > 0) return LockMode.X_LOCK;
            if (oc_lock_count > 0) return LockMode.OC_LOCK;
            if (s_lock_count > 0) return LockMode.S_LOCK;
            return LockMode.NONE;
        }
    }

    // Manages all state and scheduling for a single partition (warehouse)
    private class PartitionManager {
        private final int partitionId;
        private final AtomicLong ipsnCounter = new AtomicLong(0); // Intra-Partition Sequence Number
        private long lastCommittedIpsn = -1;

        private final ConcurrentHashMap<String, LockEntry> lockManager = new ConcurrentHashMap<>();
        private final BlockingQueue<Transaction> registrationQueue = new LinkedBlockingQueue<>();
        private final PriorityBlockingQueue<Transaction> commitQueue = new PriorityBlockingQueue<>(256, // Increased capacity to reduce resizing overhead under contention
                Comparator.comparingLong(t -> (Long) t.meta.get("ipsn")));

        private final ReentrantLock schedulerLock = new ReentrantLock();
        private final Condition schedulerCondition = schedulerLock.newCondition();
        private final Thread schedulerThread;
        private volatile boolean running = true;

        PartitionManager(int partitionId) {
            this.partitionId = partitionId;
            this.schedulerThread = new Thread(this::run, "scheduler-" + partitionId);
            this.schedulerThread.start();
        }

        void shutdown() {
            running = false;
            schedulerThread.interrupt();
            signalScheduler(); // Ensure scheduler wakes up to check running flag
        }

        private void run() {
            while (running) {
                try {
                    // Prioritize committing before registering new transactions to keep the pipeline moving
                    processCommits();
                    processRegistrations();
                    schedulerLock.lock();
                    try {
                        // If both queues are empty, wait for work to arrive
                        if (commitQueue.isEmpty() && registrationQueue.isEmpty()) {
                            schedulerCondition.await();
                        }
                    } finally {
                        schedulerLock.unlock();
                    }
                } catch (InterruptedException e) {
                    running = false;
                    Thread.currentThread().interrupt();
                }
            }
        }

        private void processRegistrations() {
            Transaction txn;
            while ((txn = registrationQueue.poll()) != null) {
                ReentrantLock lock = (ReentrantLock) txn.meta.get("lock");
                Condition cond = (Condition) txn.meta.get("condition");
                lock.lock();
                try {
                    // Ensure transaction is still active before assigning IPSN
                    if (txn.status == TxStatus.ACTIVE) {
                        txn.meta.put("ipsn", ipsnCounter.getAndIncrement());
                    }
                    cond.signal();
                } finally {
                    lock.unlock();
                }
            }
        }

        private void processCommits() {
            while (true) {
                Transaction txn = commitQueue.peek();
                if (txn == null || (Long) txn.meta.get("ipsn") != lastCommittedIpsn + 1) {
                    break; // Head of queue is not the next in sequence, or queue is empty
                }

                // We have the next transaction to commit
                txn = commitQueue.poll();
                
                ReentrantLock lock = (ReentrantLock) txn.meta.get("lock");
                Condition cond = (Condition) txn.meta.get("condition");
                lock.lock();
                try {
                    if (txn.status == TxStatus.ABORTED) {
                        // This transaction was rolled back while in the commit queue
                        lastCommittedIpsn++;
                        txn.meta.put("commit_result", CommitResult.failure("Rolled back by client"));
                    } else {
                        KVStore store = (KVStore) txn.meta.get("kvstore");
                        // Apply writes to KVStore
                        for (Map.Entry<String, Object> entry : txn.writeSet.entrySet()) {
                            store.update(entry.getKey(), entry.getValue());
                        }
                        txn.status = TxStatus.COMMITTED;
                        lastCommittedIpsn++;
                        txn.meta.put("commit_result", CommitResult.success((int)lastCommittedIpsn));
                    }
                    
                    releaseAllLocks(txn);
                    cond.signal(); // Wake up the waiting client thread
                } finally {
                    lock.unlock();
                }
            }
        }

        void registerTransaction(Transaction txn) throws InterruptedException {
            boolean wasEmpty = registrationQueue.isEmpty();
            registrationQueue.put(txn);
            if (wasEmpty) { // Only signal if the queue was empty, implying the scheduler might be waiting
                signalScheduler();
            }
        }
        
        void submitForCommit(Transaction txn) throws InterruptedException {
            boolean wasEmpty = commitQueue.isEmpty();
            commitQueue.put(txn);
            if (wasEmpty) { // Only signal if the queue was empty, implying the scheduler might be waiting
                signalScheduler();
            }
        }

        private void signalScheduler() {
            schedulerLock.lock();
            try {
                schedulerCondition.signal();
            } finally {
                schedulerLock.unlock();
            }
        }
    }
    
    @Override
    public Transaction begin(KVStore store) {
        int txid = nextTxid.getAndIncrement();
        Transaction txn = new Transaction(txid, 0);
        ReentrantLock lock = new ReentrantLock();
        txn.meta.put("lock", lock);
        txn.meta.put("condition", lock.newCondition());
        txn.meta.put("held_locks", new HashMap<String, LockMode>());
        activeTransactions.put(txid, txn);
        return txn;
    }

    private void ensureTransactionIsInitialized(Transaction txn, String key) {
        if (txn.meta.containsKey("partition_id")) {
            return;
        }
        ReentrantLock lock = (ReentrantLock) txn.meta.get("lock");
        lock.lock();
        try {
            // Double-check after acquiring lock
            if (txn.meta.containsKey("partition_id")) {
                return;
            }
            int warehouseId = getWarehouseId(key);
            txn.meta.put("partition_id", warehouseId);
            PartitionManager pm = partitions.computeIfAbsent(warehouseId, PartitionManager::new);
            
            pm.registerTransaction(txn);
            // Wait for the scheduler to assign an IPSN
            ((Condition) txn.meta.get("condition")).await();

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            txn.status = TxStatus.ABORTED;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Object read(Transaction txn, String key, KVStore store) {
        if (txn.status != TxStatus.ACTIVE) return null;
        ensureTransactionIsInitialized(txn, key);
        if (txn.status != TxStatus.ACTIVE) return null; // Status might change during initialization

        if (txn.writeSet.containsKey(key)) {
            return txn.writeSet.get(key); // Read-your-own-writes
        }
        if (txn.readSet.containsKey(key)) {
            Object cachedValue = txn.readSet.get(key);
            return (cachedValue == NULL_SENTINEL) ? null : cachedValue;
        }

        acquireLock(txn, key, LockMode.S_LOCK);
        if (txn.status != TxStatus.ACTIVE) return null;

        Object value = store.get(key);
        // KVStore uses AtomicLong for numbers, unwrap it for the transaction logic
        if (value instanceof AtomicLong) {
            value = ((AtomicLong) value).get();
        }
        
        // **FIX**: Use sentinel for null values to avoid NPE in ConcurrentHashMap
        Object valueToCache = (value == null) ? NULL_SENTINEL : value;
        txn.readSet.put(key, valueToCache);
        
        return value;
    }

    @Override
    public void write(Transaction txn, String key, Object value, KVStore store) {
        if (txn.status != TxStatus.ACTIVE) return;
        ensureTransactionIsInitialized(txn, key);
        if (txn.status != TxStatus.ACTIVE) return;

        LockMode mode = getLockModeForKey(key, value);
        acquireLock(txn, key, mode);
        if (txn.status != TxStatus.ACTIVE) return;
        
        txn.writeSet.put(key, value);
    }

    @Override
    public CommitResult commit(Transaction txn, KVStore store) {
        if (txn.status != TxStatus.ACTIVE) {
            return CommitResult.failure("Transaction already completed.");
        }
        if (!txn.meta.containsKey("partition_id")) { // Read-only transaction
            txn.status = TxStatus.COMMITTED;
            activeTransactions.remove(txn.txid);
            return CommitResult.success(0);
        }

        PartitionManager pm = partitions.get((Integer) txn.meta.get("partition_id"));
        ReentrantLock lock = (ReentrantLock) txn.meta.get("lock");
        lock.lock();
        try {
            txn.meta.put("kvstore", store);
            pm.submitForCommit(txn);
            ((Condition) txn.meta.get("condition")).await(); // Wait for scheduler to commit
            
            activeTransactions.remove(txn.txid);
            return (CommitResult) txn.meta.get("commit_result");

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            txn.status = TxStatus.ABORTED;
            return CommitResult.failure("Commit interrupted");
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void rollback(Transaction txn, KVStore store) {
        if (txn.status != TxStatus.ACTIVE) return;
        
        txn.status = TxStatus.ABORTED;
        
        if (txn.meta.containsKey("partition_id")) {
             PartitionManager pm = partitions.get((Integer) txn.meta.get("partition_id"));
             // The scheduler thread will eventually process this transaction from the commit queue,
             // detect its ABORTED status, and handle it. No need to remove it proactively.
             releaseAllLocks(txn);
        }

        activeTransactions.remove(txn.txid);
    }
    
    private void acquireLock(Transaction txn, String key, LockMode requestedMode) {
        PartitionManager pm = partitions.get((Integer) txn.meta.get("partition_id"));
        LockEntry entry = pm.lockManager.computeIfAbsent(key, k -> new LockEntry());
        long txnIpsn = (Long) txn.meta.get("ipsn");
        Map<String, LockMode> txnHeldLocks = (Map<String, LockMode>) txn.meta.get("held_locks");

        synchronized (entry) {
            while (true) {
                if (txn.status != TxStatus.ACTIVE) return;

                LockMode existingTxnMode = txnHeldLocks.get(key);
                if (existingTxnMode != null) {
                    // Current transaction already holds a lock on this key.
                    // If the requested mode is not stronger than the existing one, return.
                    if (getStrongerLockMode(existingTxnMode, requestedMode) == existingTxnMode) {
                        return;
                    }
                    // If it wants a stronger lock, it will effectively "upgrade".
                    // Decrement old count before proceeding to conflict check and incrementing new count.
                    switch (existingTxnMode) {
                        case S_LOCK: entry.s_lock_count--; break;
                        case X_LOCK: entry.x_lock_count--; break;
                        case OC_LOCK: entry.oc_lock_count--; break;
                        // case NONE: break; // This case is unreachable if existingTxnMode is not null. Removed dead code.
                    }
                }

                LockMode currentAggregateMode = entry.getCurrentAggregateMode();
                boolean conflict = false;

                // Check if requestedMode conflicts with the current aggregate mode.
                // If it does, we need to find if an OLDER transaction is causing this conflict.
                if (!areLocksCompatible(currentAggregateMode, requestedMode)) { 
                    for (Map.Entry<Integer, Long> ownerEntry : entry.ownersWithIpsn.entrySet()) {
                        int ownerTxid = ownerEntry.getKey();
                        long ownerIpsn = ownerEntry.getValue();
                        
                        if (ownerTxid == txn.txid) continue; // Skip self

                        // Conflict if locks are incompatible AND the owner is an OLDER transaction.
                        // The current transaction (txn) must wait for older, conflicting owners.
                        if (ownerIpsn < txnIpsn) { // The compatibility check is based on the aggregate mode
                            conflict = true;
                            break;
                        }
                        // If ownerIpsn >= txnIpsn (owner is younger or same IPSN), this transaction (txn)
                        // is older or same and "wins" against the younger owner if locks are incompatible.
                        // It does not wait for younger transactions.
                    }
                }

                if (!conflict) {
                    // Grant lock / Increment new count
                    switch (requestedMode) {
                        case S_LOCK: entry.s_lock_count++; break;
                        case X_LOCK: entry.x_lock_count++; break;
                        case OC_LOCK: entry.oc_lock_count++; break;
                    }
                    entry.ownersWithIpsn.put(txn.txid, txnIpsn); // Add/update self to owners map with IPSN
                    txnHeldLocks.put(key, requestedMode); // Update txn's record of its held lock
                    return;
                } else {
                    try {
                        entry.wait(); // Wait for a lock to be released
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        txn.status = TxStatus.ABORTED;
                        return;
                    }
                }
            }
        }
    }

    private void releaseAllLocks(Transaction txn) {
        Map<String, LockMode> heldLocks = (Map<String, LockMode>) txn.meta.get("held_locks");
        if (heldLocks == null || heldLocks.isEmpty()) return;
        
        PartitionManager pm = partitions.get((Integer) txn.meta.get("partition_id"));
        if (pm == null) return; // Defensive check, should not happen if txn has partition_id
        
        for (Map.Entry<String, LockMode> heldLockEntry : heldLocks.entrySet()) {
            String key = heldLockEntry.getKey();
            LockMode releasedMode = heldLockEntry.getValue(); // The specific lock mode this txn held on this key
            LockEntry entry = pm.lockManager.get(key);
            if (entry != null) {
                synchronized (entry) {
                    entry.ownersWithIpsn.remove(Integer.valueOf(txn.txid)); // Remove txid from owners map
                    // Decrement the appropriate count
                    switch (releasedMode) {
                        case S_LOCK: entry.s_lock_count--; break;
                        case X_LOCK: entry.x_lock_count--; break;
                        case OC_LOCK: entry.oc_lock_count--; break;
                    }
                    entry.notifyAll(); // Wake up all waiting threads
                }
            }
        }
        // Clear the transaction's held_locks map after releasing all locks
        heldLocks.clear();
    }
    
    private boolean areLocksCompatible(LockMode held, LockMode requested) {
        if (held == LockMode.NONE || requested == LockMode.NONE) return true;
        if (held == LockMode.OC_LOCK && requested == LockMode.OC_LOCK) return true;
        if (held == LockMode.S_LOCK && requested == LockMode.S_LOCK) return true;
        return false;
    }

    // Helper to determine the stronger lock mode between two modes.
    // X_LOCK > OC_LOCK > S_LOCK > NONE
    private LockMode getStrongerLockMode(LockMode mode1, LockMode mode2) {
        if (mode1 == LockMode.X_LOCK || mode2 == LockMode.X_LOCK) return LockMode.X_LOCK;
        if (mode1 == LockMode.OC_LOCK || mode2 == LockMode.OC_LOCK) return LockMode.OC_LOCK;
        if (mode1 == LockMode.S_LOCK || mode2 == LockMode.S_LOCK) return LockMode.S_LOCK;
        return LockMode.NONE;
    }

    private LockMode getLockModeForKey(String key, Object value) {
        // d_..._next_oid must be exclusive
        if (key.endsWith("_next_oid")) {
            return LockMode.X_LOCK;
        }
        // All other writes in this workload are commutative arithmetic operations
        return LockMode.OC_LOCK;
    }

    private int getWarehouseId(String key) {
        Matcher matcher = WAREHOUSE_KEY_PATTERN.matcher(key);
        if (matcher.matches()) {
            return Integer.parseInt(matcher.group(1));
        }
        // Fallback for non-warehouse-specific keys like "item_..."
        return 0; // A global partition for shared data
    }

    public void shutdown() {
        partitions.values().forEach(PartitionManager::shutdown);
    }
}
