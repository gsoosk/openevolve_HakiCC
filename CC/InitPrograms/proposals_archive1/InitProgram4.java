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
        private final ConcurrentHashMap<Integer, Transaction> activeTransactions = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<Transaction, Set<Transaction>> successors = new ConcurrentHashMap<>();
        private final ReentrantLock graphLock = new ReentrantLock(true);
        private final Condition globalCondition = graphLock.newCondition();

        public void register(Transaction txn) {
            graphLock.lock();
            try {
                activeTransactions.put(txn.txid, txn);
            } finally {
                graphLock.unlock();
            }
        }

        public Object handleOperation(Transaction txn, String key, Object value, boolean isWrite, KVStore store) {
            graphLock.lock();
            try {
                if (txn.status != TxStatus.ACTIVE) return null;

                // Step 1: Register intent for conflict detection. For reads, we don't fetch the value yet.
                if (isWrite) {
                    txn.writeSet.put(key, value == null ? NULL_SENTINEL : value);
                } else {
                    txn.readSet.put(key, NULL_SENTINEL); // Placeholder for conflict detection
                }

                @SuppressWarnings("unchecked")
                final Set<Transaction> txnPredecessors = (Set<Transaction>) txn.meta.get("predecessors");

                // Step 2: Discover conflicts and establish dependencies or wound younger transactions.
                for (Transaction otherTxn : activeTransactions.values()) {
                    if (otherTxn.txid == txn.txid || otherTxn.status != TxStatus.ACTIVE) continue;

                    boolean hasConflict = isWrite ?
                        (otherTxn.readSet.containsKey(key) || otherTxn.writeSet.containsKey(key)) :
                        (otherTxn.writeSet.containsKey(key));

                    if (hasConflict) {
                        if (txn.startTs > otherTxn.startTs) { // txn is younger, must wait
                            if (txnPredecessors.add(otherTxn)) {
                                successors.computeIfAbsent(otherTxn, k -> ConcurrentHashMap.newKeySet()).add(txn);
                            }
                        } else { // txn is older, must wound
                            otherTxn.status = TxStatus.ABORTED;
                        }
                    }
                }

                // Step 3: Wait until all predecessors have completed.
                while (!txnPredecessors.isEmpty()) {
                    if (txn.status != TxStatus.ACTIVE) return null;
                    
                    globalCondition.signalAll(); // Signal anyone we might have wounded
                    try {
                        globalCondition.await();
                    } catch (InterruptedException e) {
                        txn.status = TxStatus.ABORTED;
                        Thread.currentThread().interrupt();
                        return null;
                    }
                }
                
                // Step 4: After waiting, if it's a read, fetch the FRESH value from the store.
                if (!isWrite) {
                    if (txn.status != TxStatus.ACTIVE) return null;
                    Object finalReadValue = store.get(key);
                    txn.readSet.put(key, finalReadValue == null ? NULL_SENTINEL : finalReadValue);
                    return finalReadValue;
                }

                return null; // For writes
            } finally {
                graphLock.unlock();
            }
        }

        public void notifyCompletion(Transaction txn) {
            graphLock.lock();
            try {
                activeTransactions.remove(txn.txid);
                Set<Transaction> txnSuccessors = successors.remove(txn);

                if (txnSuccessors != null) {
                    for (Transaction successor : txnSuccessors) {
                        @SuppressWarnings("unchecked")
                        final Set<Transaction> successorPredecessors = (Set<Transaction>) successor.meta.get("predecessors");
                        if (successorPredecessors != null) {
                            successorPredecessors.remove(txn);
                        }
                    }
                }
                globalCondition.signalAll();
            } finally {
                graphLock.unlock();
            }
        }
    }

    @Override
    public Transaction begin(KVStore store) {
        int txid = txidCounter.incrementAndGet();
        int ts = timestampCounter.incrementAndGet();
        Transaction txn = new Transaction(txid, ts);
        txn.meta.put("predecessors", ConcurrentHashMap.newKeySet());
        txn.meta.put("notified", false);
        coordinator.register(txn);
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

        return coordinator.handleOperation(txn, key, null, false, store);
    }

    @Override
    public void write(Transaction txn, String key, Object value, KVStore store) {
        if (txn.status != TxStatus.ACTIVE) return;
        coordinator.handleOperation(txn, key, value, true, store);
    }

    @Override
    public CommitResult commit(Transaction txn, KVStore store) {
        boolean committed = false;
        
        coordinator.graphLock.lock();
        try {
            if (txn.status == TxStatus.ACTIVE) {
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
        } finally {
            coordinator.graphLock.unlock();
        }

        rollback(txn, store); // Use idempotent rollback for notification

        if (committed) {
            return CommitResult.success(timestampCounter.get());
        } else {
            return CommitResult.failure("Transaction aborted concurrently");
        }
    }

    @Override
    public void rollback(Transaction txn, KVStore store) {
        boolean alreadyNotified = (boolean) txn.meta.get("notified");
        if (alreadyNotified) {
            return;
        }
        txn.meta.put("notified", true);
        if (txn.status == TxStatus.ACTIVE) {
            txn.status = TxStatus.ABORTED;
        }
        coordinator.notifyCompletion(txn);
    }
}

// EVOLVE-BLOCK-END