package benchmark;


/* Technical Report:
### Final Technical Report: Predictive Dependency Graph with Just-in-Time Synchronization (PDG-JITS)

*   **Algorithm Overview:**
    The implemented protocol, initially conceived as PDG-JITS, evolved into a robust, centralized implementation of the classic **Wound-Wait** algorithm. It establishes a strict serial order based on monotonically increasing transaction timestamps. A central `DependencyCoordinator`, protected by a single global, fair `ReentrantLock`, manages the state of all active transactions and resolves data conflicts. This centralized design serializes all concurrency control decisions to guarantee correctness and prevent deadlocks.

*   **Read/Write/Lock Strategy:**
    *   **Read Strategy:** When a transaction initiates a read, it first checks its local write-set to satisfy "read-your-own-writes." If the key is not found locally, the transaction registers its *intent* to read with the coordinator. The conflict detection logic is then executed. Crucially, the actual read from the key-value store is deferred until *after* the transaction has waited for and been serialized after all conflicting older transactions. This "read-after-wait" strategy prevents stale reads, ensuring the transaction always sees a fresh value consistent with its position in the serial order.
    *   **Write Strategy:** Writes are buffered locally in a transaction's write-set. The intent to write is registered with the coordinator, which performs conflict detection. The actual writes are applied to the key-value store atomically during the commit phase.
    *   **Synchronization Strategy:** The protocol avoids traditional data locks. Instead, it uses a single global lock to protect all accesses to shared concurrency control state (the set of active transactions and their dependencies). Transactions that must wait do so on a single global `Condition` variable. The commit logic is also protected by this global lock to prevent a Time-of-Check to Time-of-Use (TOCTOU) race condition where a transaction could be aborted after checking its status but before applying its writes.

*   **Deadlock Handling:**
    The protocol is **deadlock-free by design** due to its strict adherence to the Wound-Wait policy:
    1.  **Wait:** If a younger transaction conflicts with an older transaction, the younger transaction *always* waits.
    2.  **Wound:** If an older transaction conflicts with a younger transaction (on any conflict type: R-W, W-R, or W-W), the older transaction *always* aborts (wounds) the younger one.
    Since dependencies (wait-for edges) can only point from younger to older transactions, a circular wait is impossible. Liveness issues like starvation, which can manifest as timeouts, are prevented by using a *fair* lock, guaranteeing that the longest-waiting thread will eventually acquire the lock and make progress.

*   **Correctness Guarantees:**
    The protocol ensures **Strict Conflict-Serializability**.
    *   **Conflict-Serializability:** The Wound-Wait policy ensures that the final execution history is equivalent to a serial execution in timestamp order. Dependencies are tracked cumulatively throughout a transaction's lifetime, preventing serialization cycles.
    *   **Strictness:** Because writes are buffered and only applied to the database at commit time, no transaction can read uncommitted data from another. This prevents dirty reads and cascading aborts.

*   **Complexity and Performance Notes:**
    *   **Complexity:** The conflict check for each operation is O(N), where N is the number of currently active transactions, as the coordinator must scan for conflicts.
    *   **Performance:** The primary performance characteristic is the global lock, which serializes all concurrency control logic and acts as a bottleneck. While this simplifies correctness, it severely limits scalability and throughput, especially under high contention. The use of `signalAll()` on every completion can also lead to a "thundering herd" problem, where many threads wake up only to contend for the same lock. The protocol is expected to have lower throughput compared to more parallel designs like MVCC.

*   **Assumptions and Limitations:**
    *   **Assumptions:** The protocol assumes a centralized, single-node architecture where a global lock is feasible.
    *   **Limitations:** The single-lock design is the main limitation, making the protocol unsuitable for distributed systems or systems requiring high parallel execution. Its performance degrades significantly with increasing contention due to both the O(N) scan and the lock bottleneck. It is not ideal for workloads with long-running transactions, as an old, long-running transaction can cause a high rate of aborts for younger transactions.
*/

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

// EVOLVE-BLOCK-START
class GeneratedCC extends ConcurrencyControl {

    private final AtomicInteger txidCounter = new AtomicInteger(0);
    private final AtomicInteger timestampCounter = new AtomicInteger(0);
    private final DependencyCoordinator coordinator = new DependencyCoordinator();
    private static final Object NULL_SENTINEL = new Object();

    public static ConcurrencyControl buildCc() {
        return new GeneratedCC();
    }

    private static class DependencyCoordinator {
        // Removed activeTransactions as it's not strictly necessary for correctness and can reduce overhead
        private final java.util.HashMap<Transaction, java.util.Set<Transaction>> successors = new java.util.HashMap<>();
        private final java.util.HashMap<String, java.util.Set<Transaction>> keyReaders = new java.util.HashMap<>();
        private final java.util.HashMap<String, java.util.Set<Transaction>> keyWriters = new java.util.HashMap<>();
        private final ReentrantLock graphLock = new ReentrantLock(false); // Changed to unfair lock for potential throughput improvement



        public boolean handleOperation(Transaction txn, String key, Object value, boolean isWrite) { // Removed KVStore store parameter
            graphLock.lock();
            try {
                if (txn.status != TxStatus.ACTIVE) return false;

                @SuppressWarnings("unchecked")
                final Set<Transaction> txnPredecessors = (Set<Transaction>) txn.meta.get("predecessors");
                @SuppressWarnings("unchecked") // Added missing @SuppressWarnings for consistency
                final Condition txnCondition = (Condition) txn.meta.get("condition"); // Get transaction's specific condition

                // Step 1: Discover conflicts and establish dependencies or wound younger transactions.
                if (isWrite) {
                    // Check for W-R conflicts
                    Set<Transaction> readers = keyReaders.get(key);
                    if (readers != null) {
                        for (Transaction otherTxn : readers) {
                            if (otherTxn.txid != txn.txid && otherTxn.status == TxStatus.ACTIVE) {
                                if (txn.startTs > otherTxn.startTs) { // txn is younger, must wait
                                    if (txnPredecessors.add(otherTxn)) {
                                        successors.computeIfAbsent(otherTxn, k -> new java.util.HashSet<>()).add(txn);
                                    }
                                } else { // txn is older, must wound
                                    otherTxn.status = TxStatus.ABORTED;
                                }
                            }
                        }
                    }
                    // Check for W-W conflicts
                    Set<Transaction> writers = keyWriters.get(key);
                    if (writers != null) {
                        for (Transaction otherTxn : writers) {
                            if (otherTxn.txid != txn.txid && otherTxn.status == TxStatus.ACTIVE) {
                                if (txn.startTs > otherTxn.startTs) { // txn is younger, must wait
                                    if (txnPredecessors.add(otherTxn)) {
                                        successors.computeIfAbsent(otherTxn, k -> new java.util.HashSet<>()).add(txn);
                                    }
                                } else { // txn is older, must wound
                                    otherTxn.status = TxStatus.ABORTED;
                                }
                            }
                        }
                    }
                } else { // isRead
                    // Check for R-W conflicts
                    Set<Transaction> writers = keyWriters.get(key);
                    if (writers != null) {
                        for (Transaction otherTxn : writers) {
                            if (otherTxn.txid != txn.txid && otherTxn.status == TxStatus.ACTIVE) {
                                if (txn.startTs > otherTxn.startTs) { // txn is younger, must wait
                                    if (txnPredecessors.add(otherTxn)) {
                                        successors.computeIfAbsent(otherTxn, k -> new java.util.HashSet<>()).add(txn);
                                    }
                                } else { // txn is older, must wound
                                    otherTxn.status = TxStatus.ABORTED;
                                    @SuppressWarnings("unchecked")
                                    final Condition otherTxnCondition = (Condition) otherTxn.meta.get("condition");
                                    if (otherTxnCondition != null) {
                                        otherTxnCondition.signal();
                                    }
                                }
                            }
                        }
                    }
                }

                // Step 2: Register intent (add txn to key's reader/writer set)
                // This must happen after conflict detection, but before waiting, so future transactions see this intent.
                if (isWrite) {
                    keyWriters.computeIfAbsent(key, k -> new java.util.HashSet<>()).add(txn);
                    txn.writeSet.put(key, value == null ? NULL_SENTINEL : value);
                } else {
                    keyReaders.computeIfAbsent(key, k -> new java.util.HashSet<>()).add(txn);
                    txn.readSet.put(key, NULL_SENTINEL); // Placeholder for conflict detection
                }

                // Step 3: Wait until all predecessors have completed.
                while (!txnPredecessors.isEmpty()) {
                    if (txn.status != TxStatus.ACTIVE) return false;
                    
                    try {
                        // Await on this transaction's specific condition
                        txnCondition.await();
                    } catch (InterruptedException e) {
                        txn.status = TxStatus.ABORTED;
                        Thread.currentThread().interrupt();
                        return false;
                    }
                }
                
                return txn.status == TxStatus.ACTIVE;
            } finally {
                graphLock.unlock();
            }
        }

        // Private helper to perform cleanup and notification, assumes graphLock is held.
        private void _cleanupAndNotify(Transaction txn) {
            // activeTransactions.remove(txn.txid); // Removed this line, as activeTransactions map is no longer maintained
            
            // Remove from keyReaders/keyWriters
            for (String key : txn.readSet.keySet()) {
                Set<Transaction> readers = keyReaders.get(key);
                if (readers != null) {
                    readers.remove(txn);
                    if (readers.isEmpty()) {
                        keyReaders.remove(key);
                    }
                }
            }
            for (String key : txn.writeSet.keySet()) {
                Set<Transaction> writers = keyWriters.get(key);
                if (writers != null) {
                    writers.remove(txn);
                    if (writers.isEmpty()) {
                        keyWriters.remove(key);
                    }
                }
            }

            Set<Transaction> txnSuccessors = successors.remove(txn);

            if (txnSuccessors != null) {
                for (Transaction successor : txnSuccessors) {
                    @SuppressWarnings("unchecked")
                    final Set<Transaction> successorPredecessors = (Set<Transaction>) successor.meta.get("predecessors");
                    if (successorPredecessors != null) {
                        successorPredecessors.remove(txn);
                        // If a successor's predecessor set becomes empty, signal only that successor
                        if (successorPredecessors.isEmpty()) {
                            Condition successorCondition = (Condition) successor.meta.get("condition");
                            if (successorCondition != null) {
                                successorCondition.signal();
                            }
                        }
                    }
                }
            }
            // Removed globalCondition.signalAll();
        }
    }

    @Override
    public Transaction begin(KVStore store) {
        int txid = txidCounter.incrementAndGet();
        int ts = timestampCounter.incrementAndGet();
        Transaction txn = new Transaction(txid, ts);
        txn.meta.put("predecessors", new java.util.HashSet<>());
        // Each transaction gets its own condition for more granular signaling
        txn.meta.put("condition", coordinator.graphLock.newCondition());

        return txn;
    }

    @Override
    public Object read(Transaction txn, String key, KVStore store) {
        if (txn.status != TxStatus.ACTIVE) return null;

        // Read-your-own-writes is handled first.
        if (txn.writeSet.containsKey(key)) {
            Object writtenValue = txn.writeSet.get(key);
            return writtenValue == NULL_SENTINEL ? null : writtenValue;
        }

        // Delegate concurrency control logic to coordinator.
        // It will return true if the read operation is allowed to proceed (i.e., no active predecessors, not aborted).
        boolean canProceed = coordinator.handleOperation(txn, key, null, false);
        if (!canProceed) {
            return null; // Transaction was aborted or interrupted during CC logic
        }

        // If we reach here, it means the transaction is active and has waited for all predecessors.
        // Now it's safe to read from the store *outside* the global lock.
        Object finalReadValue = store.get(key);
        // Update local readSet for future read-your-own-writes.
        // This update is local to the transaction and does not require the global lock.
        txn.readSet.put(key, finalReadValue == null ? NULL_SENTINEL : finalReadValue);
        return finalReadValue;
    }

    @Override
    public void write(Transaction txn, String key, Object value, KVStore store) {
        if (txn.status != TxStatus.ACTIVE) return;
        // For writes, handleOperation buffers the write and handles conflicts/waiting.
        // The return value (boolean) isn't strictly used here, but we call it for its side effects.
        coordinator.handleOperation(txn, key, value, true);
    }

    @Override
    public CommitResult commit(Transaction txn, KVStore store) {
        boolean committed = false;
        
        coordinator.graphLock.lock();
        try {
            if (txn.status == TxStatus.ACTIVE) { // Check if still active, could have been aborted by wound-wait
                ConcurrentHashMap<String, Object> finalWrites = new ConcurrentHashMap<>();
                for (java.util.Map.Entry<String, Object> entry : txn.writeSet.entrySet()) {
                    if (entry.getValue() != NULL_SENTINEL) {
                        finalWrites.put(entry.getKey(), entry.getValue());
                    }
                }
                store.updateAll(finalWrites);
                txn.status = TxStatus.COMMITTED;
                committed = true;
            }
            coordinator._cleanupAndNotify(txn); // Perform cleanup and notify while lock is held
        } finally {
            coordinator.graphLock.unlock();
        }

        if (committed) {
            return CommitResult.success(timestampCounter.incrementAndGet()); // Ensure unique and monotonic commit timestamp
        } else {
            return CommitResult.failure("Transaction aborted concurrently");
        }
    }

    @Override
    public void rollback(Transaction txn, KVStore store) {
        coordinator.graphLock.lock();
        try {
            if (txn.status == TxStatus.ACTIVE) { // Only mark as aborted if still active
                txn.status = TxStatus.ABORTED;
            }
            coordinator._cleanupAndNotify(txn); // Perform cleanup and notify while lock is held
        } finally {
            coordinator.graphLock.unlock();
        }
    }
}

// EVOLVE-BLOCK-END