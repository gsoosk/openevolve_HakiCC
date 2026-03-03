/* Technical Report:
### Final Report: Hierarchical Speculative Locking (HSL)

**1. Algorithm Overview**

Hierarchical Speculative Locking (HSL) is a hybrid concurrency control protocol specifically optimized for workloads where data access can be mapped to coarse-grained logical entities. The protocol bifurcates transaction execution into two phases: a highly concurrent, non-blocking speculative phase, and a pessimistic, serialized commit phase.

1.  **Entity Mapping:** A key-to-entity mapping function groups granular keys (e.g., `i_123_name`, `i_123_status`) into a single logical entity (e.g., `item_123`). All concurrency control operates at this entity level.
2.  **Speculative Execution:** Transactions execute optimistically. They register their "intent" to access an entity in a global map but do not acquire any locks. Reads and writes proceed without blocking, and writes are buffered locally.
3.  **Pessimistic Commit with Proactive Invalidation:** Before a writer transaction can commit, it must acquire exclusive locks on all entities it touched. Once locks are held, it applies its writes and then *proactively aborts* all other concurrent transactions that were speculating on the same entities. This "winner-takes-all" approach resolves conflicts by invalidating losers rather than relying on them to self-validate.

**2. Read/Write/Lock or Validation Strategy**

*   **Read/Write Strategy:** During the speculative phase, reads and writes are non-blocking. Upon each operation, the transaction maps the key to an entity, registers its ID as a "speculator" for that entity, and adds the entity to its local `touchedEntities` set. Reads are served from the KV store (or the transaction's local write-set), and writes are buffered locally. Transactions check their status before each operation and halt if they have been remotely aborted.

*   **Locking and Validation Strategy:** The commit phase is serialized by a single global lock to ensure atomicity.
    *   **Read-Only Transactions:** Have a fast path; they acquire the global commit lock, check they haven't been aborted, and commit immediately.
    *   **Writer Transactions:** After acquiring the global commit lock, they acquire exclusive, fine-grained locks for each entity they touched. Once all entity locks are held, they apply writes and then perform the proactive invalidation step. The combination of a global commit lock and fine-grained entity locks ensures that the invalidation logic is correct and does not suffer from race conditions.

**3. Deadlock Handling**

The protocol is **deadlock-free**. Deadlocks that could arise from acquiring multiple fine-grained entity locks are prevented by enforcing a **globally consistent lock acquisition order**. All transactions sort their list of touched entities lexicographically before attempting to acquire the locks, thus breaking any potential cyclic dependencies.

**4. Correctness Guarantees**

HSL guarantees **Strict Serializability**.
*   **Serializability:** The serialization point for all transactions is the acquisition of the global commit lock. Since only one transaction can be in this critical section at a time, a total serial order is established. The proactive invalidation mechanism ensures that no transaction can commit based on a stale state; if the state it read was changed by a concurrent transaction T1, T1 would have aborted it during its own commit phase before the current transaction could reach its commit decision.
*   **Strictness:** Writes are only applied to the database after all necessary locks are held and the transaction is guaranteed to commit. This prevents any transaction from reading uncommitted data.

**5. Complexity and Performance Notes**

*   **Complexity:** The implementation complexity is moderate. It requires a robust key-to-entity mapping function and careful management of global state (speculator map, active transactions). The logic is simpler than a full SSI implementation as it avoids complex read-set validation.
*   **Performance:** HSL shows exceptional performance on its target workload (AuctionMark) where transactions are well-scoped to entities. The non-blocking speculative phase allows for high parallelism. The main performance bottleneck is the global commit lock, which serializes all commits. However, for workloads with short transactions and high contention, the cost of this lock is outweighed by the benefit of avoiding numerous aborts from optimistic validation failures. The coarse-grained entity locking reduces metadata overhead compared to per-key locking.

**6. Assumptions and Limitations**

*   **Assumptions:** The protocol's effectiveness is highly dependent on a meaningful and efficient key-to-entity mapping. It assumes that most contention occurs between transactions operating on the same logical object.
*   **Limitations:** The primary limitation is the scalability bottleneck imposed by the global commit lock. On workloads with very high commit rates and many cores, this lock will become the limiting factor. The protocol's performance may degrade on workloads where transactions touch many entities with no clear locality, as this increases the number of fine-grained locks to acquire and widens the scope of proactive invalidation, potentially leading to high abort rates.Novelty Result:
*/


package benchmark;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Implements the Hierarchical Speculative Locking (HSL) protocol.
 * HSL allows transactions to execute speculatively without locks and enforces
 * serializability at commit time through coarse-grained entity locking and
 * proactive invalidation of conflicting transactions.
 *
 * This revised version introduces a global commit lock to fix a race condition
 * that violated serializability in the original implementation.
 */
class GeneratedCC extends ConcurrencyControl {

    private static final AtomicInteger txidCounter = new AtomicInteger(0);
    private static final AtomicInteger timestampCounter = new AtomicInteger(0);

    // Global map of all active transactions, allowing one to invalidate another.
    private static final ConcurrentHashMap<Integer, Transaction> activeTransactions = new ConcurrentHashMap<>();

    // Maps entity ID to a thread-safe set of speculating transaction IDs.
    private static final ConcurrentHashMap<String, ConcurrentHashMap.KeySetView<Integer, Boolean>> speculativeMap = new ConcurrentHashMap<>();

    // Maps entity ID to a ReentrantLock for the pessimistic commit phase.
    private static final ConcurrentHashMap<String, ReentrantLock> firmLocks = new ConcurrentHashMap<>();

    // A global lock to serialize all commit decisions, preventing race conditions.
    private static final ReentrantLock commitLock = new ReentrantLock();

    // Metadata key for the Transaction.meta map
    private static final String META_TOUCHED_ENTITIES = "touchedEntities";

    public static ConcurrencyControl buildCc() {
        return new GeneratedCC();
    }

    private String getEntityId(String key) {
        try {
            String[] parts = key.split("_");
            if (parts.length < 2) return key; // Fallback for simple keys

            switch (parts[0]) {
                case "u":
                    return "user_" + parts[1];
                case "i":
                case "ib":
                case "imb":
                case "ic":
                case "if":
                    if (parts.length > 2) return "item_" + parts[1] + "_" + parts[2];
                    break;
                case "cnt": // "cnt_ic_..."
                    if (parts.length > 3) return "item_" + parts[2] + "_" + parts[3];
                    break;
                case "ip":
                    return "purchase_" + parts[1];
                case "ui":
                     if (parts.length > 2) return "user_item_" + parts[1] + "_" + parts[2];
                     break;
            }
        } catch (Exception e) {
            return key; // Fallback for malformed or unexpected keys
        }
        return key; // Fallback for keys that don't match patterns
    }

    @Override
    public Transaction begin(KVStore store) {
        int txid = txidCounter.incrementAndGet();
        Transaction txn = new Transaction(txid, timestampCounter.incrementAndGet());
        txn.meta.put(META_TOUCHED_ENTITIES, ConcurrentHashMap.newKeySet());
        activeTransactions.put(txid, txn);
        return txn;
    }

    private void registerSpeculativeIntent(Transaction txn, String key) {
        String entityId = getEntityId(key);
        speculativeMap.computeIfAbsent(entityId, k -> ConcurrentHashMap.newKeySet()).add(txn.txid);
        ((Set<String>) txn.meta.get(META_TOUCHED_ENTITIES)).add(entityId);
    }

    @Override
    public Object read(Transaction txn, String key, KVStore store) {
        if (txn.status != TxStatus.ACTIVE) return null;

        registerSpeculativeIntent(txn, key);

        if (txn.writeSet.containsKey(key)) {
            return txn.writeSet.get(key);
        }
        return store.get(key);
    }

    @Override
    public void write(Transaction txn, String key, Object value, KVStore store) {
        if (txn.status != TxStatus.ACTIVE) return;

        registerSpeculativeIntent(txn, key);
        txn.writeSet.put(key, value);
    }

    @Override
    public CommitResult commit(Transaction txn, KVStore store) {
        if (txn.status != TxStatus.ACTIVE) {
            return CommitResult.failure("Transaction was already aborted before commit phase.");
        }

        List<ReentrantLock> acquiredLocks = new ArrayList<>();
        commitLock.lock();
        try {
            if (txn.status != TxStatus.ACTIVE) {
                rollback(txn, store); // Ensure cleanup if aborted while waiting
                return CommitResult.failure("Transaction was aborted while waiting for commit lock.");
            }

            // Read-only path
            if (txn.writeSet.isEmpty()) {
                txn.status = TxStatus.COMMITTED;
                cleanup(txn);
                return CommitResult.success(timestampCounter.incrementAndGet());
            }

            // Writer path
            Set<String> touchedEntities = (Set<String>) txn.meta.get(META_TOUCHED_ENTITIES);
            List<String> sortedEntities = new ArrayList<>(touchedEntities);
            Collections.sort(sortedEntities);

            for (String entityId : sortedEntities) {
                ReentrantLock lock = firmLocks.computeIfAbsent(entityId, k -> new ReentrantLock());
                lock.lock();
                acquiredLocks.add(lock);
            }

            store.updateAll(txn.writeSet);

            for (String entityId : touchedEntities) {
                Set<Integer> speculators = speculativeMap.get(entityId);
                if (speculators != null) {
                    for (int speculatorTxid : new ArrayList<>(speculators)) { // Iterate on copy
                        if (speculatorTxid != txn.txid) {
                            Transaction specTxn = activeTransactions.get(speculatorTxid);
                            if (specTxn != null) {
                                specTxn.status = TxStatus.ABORTED;
                            }
                        }
                    }
                }
            }

            txn.status = TxStatus.COMMITTED;
            cleanup(txn);
            return CommitResult.success(timestampCounter.incrementAndGet());

        } finally {
            for (ReentrantLock lock : acquiredLocks) {
                lock.unlock();
            }
            commitLock.unlock();
        }
    }

    @Override
    public void rollback(Transaction txn, KVStore store) {
        if (txn.status == TxStatus.COMMITTED) return;
        txn.status = TxStatus.ABORTED;
        cleanup(txn);
    }

    private void cleanup(Transaction txn) {
        Set<String> touchedEntities = (Set<String>) txn.meta.get(META_TOUCHED_ENTITIES);
        if (touchedEntities != null) {
            for (String entityId : touchedEntities) {
                Set<Integer> speculators = speculativeMap.get(entityId);
                if (speculators != null) {
                    speculators.remove(txn.txid);
                }
            }
        }
        activeTransactions.remove(txn.txid);
    }
}
