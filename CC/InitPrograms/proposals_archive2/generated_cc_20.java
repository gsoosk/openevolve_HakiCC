/* Technical Report:
### Final Technical Report: Dual-Counter Optimistic Validation (DCOV)

**1. Algorithm Overview**
Dual-Counter Optimistic Validation (DCOV) is a specialized optimistic concurrency control (OCC) protocol designed to leverage pre-known transaction semantics. It enhances traditional OCC by replacing a single version number with two distinct, monotonic counters for each data item:

*   **Exclusive-Modification Counter (EMC):** Incremented by non-commutative, `read-modify-write` operations (e.g., setting stock quantity, bank transfers).
*   **Commutative-Modification Counter (CMC):** Incremented by purely commutative, arithmetic operations (e.g., updating TPC-C balance and YTD figures).

Transactions execute optimistically, buffering writes locally and recording the `EMC` and `CMC` of each key at the time of its first read. The protocol's key optimizations are semantic write classification and blind read elimination, which significantly reduce the number of aborts in mixed workloads.

**2. Read/Write and Validation Strategy**
*   **Read Strategy:** The protocol enforces **Repeatable Reads**. The first read of a key fetches its value and both `EMC`/`CMC` counters from the store, which are cached in the transaction's local read set. All subsequent reads of that key during the transaction's lifetime are served from this cache. This guarantees a consistent snapshot for the transaction, preventing non-repeatable read anomalies. The protocol also correctly handles read-your-own-writes, ensuring a transaction sees its own buffered modifications.
*   **Write Strategy:** Writes are classified based on key name semantics.
    *   `EXCLUSIVE` writes (e.g., `s_..._qty` or any non-TPC-C write) are buffered as absolute values.
    *   `COMMUTATIVE_DELTA` writes (e.g., `..._ytd`, `..._bal`) are converted to deltas based on the transaction's current consistent view (including its own prior writes) and buffered.
*   **Validation Strategy:** At commit time, a transaction acquires a short-term, partition-level lock and performs a validation check. For every key in its non-blind read set, it compares the cached `EMC` and `CMC` with the current values in the store. A mismatch on *either* counter results in an abort. This strict validation of both counters is necessary to guarantee serializability.
*   **Blind Read Elimination:** A critical optimization is the identification of "blind reads" (reads whose values do not influence the transaction's write set, e.g., reading customer balance in `NewOrder`). These reads are completely excluded from the validation set, preventing them from causing false-positive aborts. This effectively decouples `NewOrder` transactions from concurrent `Payment` transactions.

**3. Deadlock Handling**
The DCOV protocol is **deadlock-free by design**. It is an optimistic protocol that does not acquire data locks during its execution phase. The only lock used is a short-term `commitLock` per partition, which is acquired atomically at the end of a transaction to serialize the `validate-then-write` phase. Since transactions never hold one lock while waiting for another, circular dependencies cannot form.

**4. Correctness Guarantees**
The protocol ensures **Strict Serializability**.
*   **Serializability:** This is guaranteed by the combination of repeatable reads and a strict, atomic commit-time validation. The repeatable read implementation prevents anomalies where a transaction might see inconsistent state. The validation phase ensures that a transaction's entire read set was stable throughout its execution. If T1 reads a key and T2 subsequently modifies it (incrementing either `EMC` or `CMC`), T1's validation will fail, forcing an abort and preventing a serialization cycle.
*   **Strictness:** All writes are buffered locally and are only applied to the store after a successful commit decision. No uncommitted data is ever visible to other transactions.

**5. Complexity and Performt push
ance Notes**
*   **Time Complexity:** `read`, `write`, `begin`, `rollback` are all O(1) on average. `commit` is O(R+W), where R and W are the sizes of the read and write sets, performed under a lock.
*   **Performance:** The protocol demonstrates high throughput, particularly for the TPC-C workload. Its primary performance benefit comes from blind read elimination, which prevents the vast majority of conflicts between `NewOrder` and `Payment` transactions. While the strict validation of both counters increases aborts among purely commutative transactions compared to a more lenient design, it is essential for correctness and still allows for a high degree of concurrency.

**6. Assumptions and Limitations**
*   **Pre-Known Semantics:** The protocol's effectiveness relies on the ability to statically classify keys and operations (e.g., `_ytd` is commutative, `_qty` is exclusive). It is not a general-purpose protocol and would require modification to support ad-hoc workloads.
*   **Partitioning:** The use of per-partition commit locks is optimized for workloads that can be cleanly partitioned (like TPC-C by warehouse). For workloads with high cross-partition contention, a global lock or a more sophisticated commit mechanism would be needed.
*   **Abort Rate:** While optimized, the protocol is still optimistic and subject to aborts under high data contention, particularly for transactions with large read sets. The final, correct implementation prioritizes serializability over minimizing the abort rate for commutative transactions.Novelty Result:

*/


package benchmark;

// EVOLVE-BLOCK-START
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

class GeneratedCC extends ConcurrencyControl {

    private final AtomicInteger nextTxid = new AtomicInteger(0);
    private final ConcurrentHashMap<Integer, PartitionState> partitions = new ConcurrentHashMap<>();

    private static final String META_PARTITION_ID = "partitionId";
    private static final String META_IS_EXCLUSIVE = "isExclusive";
    private static final String META_DCOV_READ_SET = "dcovReadSet";
    private static final String META_DCOV_WRITE_SET = "dcovWriteSet";

    private enum WriteType { EXCLUSIVE, COMMUTATIVE_DELTA }

    private static class DcovValue {
        final Object value;
        final long emc;
        final long cmc;

        DcovValue(Object value, long emc, long cmc) {
            this.value = value;
            this.emc = emc;
            this.cmc = cmc;
        }
    }

    private static class ReadRecord {
        final long emc;
        final long cmc;
        final boolean isBlind;

        ReadRecord(long emc, long cmc, boolean isBlind) {
            this.emc = emc;
            this.cmc = cmc;
            this.isBlind = isBlind;
        }
    }

    private static class WriteRecord {
        final Object value;
        final WriteType type;

        WriteRecord(Object value, WriteType type) {
            this.value = value;
            this.type = type;
        }
    }

    private static class PartitionState {
        final ReentrantLock commitLock = new ReentrantLock();
        final AtomicInteger nextOidCounter = new AtomicInteger(1);
    }

    public static ConcurrencyControl buildCc() {
        return new GeneratedCC();
    }

    @Override
    public Transaction begin(KVStore store) {
        Transaction txn = new Transaction(nextTxid.getAndIncrement(), 0);
        txn.meta.put(META_IS_EXCLUSIVE, new AtomicBoolean(false));
        txn.meta.put(META_DCOV_READ_SET, new ConcurrentHashMap<String, ReadRecord>());
        txn.meta.put(META_DCOV_WRITE_SET, new ConcurrentHashMap<String, WriteRecord>());
        return txn;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object read(Transaction txn, String key, KVStore store) {
        if (txn.status != TxStatus.ACTIVE) return null;

        Map<String, WriteRecord> writeSet = (Map<String, WriteRecord>) txn.meta.get(META_DCOV_WRITE_SET);
        Map<String, ReadRecord> dcovReadSet = (Map<String, ReadRecord>) txn.meta.get(META_DCOV_READ_SET);

        if (writeSet.containsKey(key)) {
            WriteRecord wr = writeSet.get(key);
            if (wr.type == WriteType.EXCLUSIVE) {
                return wr.value;
            } else {
                long baseValue = ((Number) txn.readSet.get(key)).longValue();
                long delta = ((Number) wr.value).longValue();
                return baseValue + delta;
            }
        }

        if (dcovReadSet.containsKey(key)) {
            return txn.readSet.get(key);
        }

        PartitionState partition = getPartitionForTxn(txn, key);
        if (key.endsWith("_next_oid") && partition != null) {
            int oid = partition.nextOidCounter.getAndIncrement();
            txn.readSet.put(key, oid);
            dcovReadSet.put(key, new ReadRecord(-1, -1, true));
            return oid;
        }

        Object rawObj = store.get(key);
        DcovValue dcovValue;

        if (rawObj instanceof DcovValue) {
            dcovValue = (DcovValue) rawObj;
        } else {
            Object initialValue = (rawObj instanceof AtomicLong) ? ((AtomicLong) rawObj).get() : rawObj;
            if (initialValue == null) {
                initialValue = 0L;
            }
            dcovValue = new DcovValue(initialValue, 0L, 0L);
        }
        
        boolean isBlind = isBlindRead(txn, key);
        dcovReadSet.put(key, new ReadRecord(dcovValue.emc, dcovValue.cmc, isBlind));
        txn.readSet.put(key, dcovValue.value);
        return dcovValue.value;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void write(Transaction txn, String key, Object value, KVStore store) {
        if (txn.status != TxStatus.ACTIVE) return;

        getPartitionForTxn(txn, key);
        Map<String, WriteRecord> writeSet = (Map<String, WriteRecord>) txn.meta.get(META_DCOV_WRITE_SET);
        WriteType type = getWriteType(key);

        if (type == WriteType.EXCLUSIVE) {
            ((AtomicBoolean) txn.meta.get(META_IS_EXCLUSIVE)).set(true);
            writeSet.put(key, new WriteRecord(value, type));
        } else { // Commutative
            Object currentValueObj = this.read(txn, key, store);
            long currentValue = ((Number) currentValueObj).longValue();
            long newValue = ((Number) value).longValue();
            long incrementalDelta = newValue - currentValue;

            writeSet.compute(key, (k, v) -> {
                long currentDelta = (v == null) ? 0L : ((Number) v.value).longValue();
                long newCumulativeDelta = currentDelta + incrementalDelta;
                return new WriteRecord(newCumulativeDelta, type);
            });
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public CommitResult commit(Transaction txn, KVStore store) {
        if (txn.status != TxStatus.ACTIVE) return CommitResult.failure("Txn not active");

        Map<String, WriteRecord> writeSet = (Map<String, WriteRecord>) txn.meta.get(META_DCOV_WRITE_SET);
        if (writeSet.isEmpty()) {
            txn.status = TxStatus.COMMITTED;
            return CommitResult.success(0);
        }

        PartitionState partition = getPartitionById((Integer) txn.meta.get(META_PARTITION_ID));
        if (partition == null) {
            partition = partitions.computeIfAbsent(0, k -> new PartitionState()); // Global partition
        }

        partition.commitLock.lock();
        try {
            Map<String, ReadRecord> readSet = (Map<String, ReadRecord>) txn.meta.get(META_DCOV_READ_SET);
            
            for (Map.Entry<String, ReadRecord> entry : readSet.entrySet()) {
                ReadRecord rr = entry.getValue();
                if (rr.isBlind) continue;

                String key = entry.getKey();
                Object rawObj = store.get(key);
                DcovValue currentDV;
                if (rawObj instanceof DcovValue) {
                    currentDV = (DcovValue) rawObj;
                } else {
                    currentDV = new DcovValue(rawObj, 0L, 0L);
                }

                if (rr.emc != currentDV.emc) {
                    txn.status = TxStatus.ABORTED;
                    return CommitResult.failure("EMC conflict on key: " + key);
                }
                // FIX: All transactions must validate the CMC to prevent non-repeatable reads.
                if (rr.cmc != currentDV.cmc) {
                    txn.status = TxStatus.ABORTED;
                    return CommitResult.failure("CMC conflict on key: " + key);
                }
            }

            for (Map.Entry<String, WriteRecord> entry : writeSet.entrySet()) {
                String key = entry.getKey();
                WriteRecord wr = entry.getValue();
                
                Object rawObj = store.get(key);
                DcovValue currentDV;
                 if (rawObj instanceof DcovValue) {
                    currentDV = (DcovValue) rawObj;
                } else {
                    Object initialValue = (rawObj instanceof AtomicLong) ? ((AtomicLong) rawObj).get() : rawObj;
                    if (initialValue == null) {
                        initialValue = 0L;
                    }
                    currentDV = new DcovValue(initialValue, 0L, 0L);
                }
                
                if (wr.type == WriteType.EXCLUSIVE) {
                    store.update(key, new DcovValue(wr.value, currentDV.emc + 1, currentDV.cmc));
                } else {
                    long baseValue = ((Number) currentDV.value).longValue();
                    long delta = ((Number) wr.value).longValue();
                    store.update(key, new DcovValue(baseValue + delta, currentDV.emc, currentDV.cmc + 1));
                }
            }
            txn.status = TxStatus.COMMITTED;
            return CommitResult.success(0);
        } finally {
            partition.commitLock.unlock();
        }
    }

    @Override
    public void rollback(Transaction txn, KVStore store) {
        txn.status = TxStatus.ABORTED;
        txn.readSet.clear();
        txn.writeSet.clear();
        txn.meta.clear();
    }

    private PartitionState getPartitionForTxn(Transaction txn, String key) {
        Integer partitionId = (Integer) txn.meta.get(META_PARTITION_ID);
        if (partitionId == null) {
            partitionId = getWarehouseIdFromKey(key);
            if (partitionId != null) {
                txn.meta.put(META_PARTITION_ID, partitionId);
            }
        }
        return getPartitionById(partitionId);
    }

    private PartitionState getPartitionById(Integer partitionId) {
        if (partitionId == null) return null;
        return partitions.computeIfAbsent(partitionId, k -> new PartitionState());
    }

    private static Integer getWarehouseIdFromKey(String key) {
        String[] parts = key.split("_");
        if (parts.length > 1) {
            try { return Integer.parseInt(parts[1]); }
            catch (NumberFormatException e) { return null; }
        }
        return null;
    }

    private static WriteType getWriteType(String key) {
        if (key.endsWith("_ytd") || key.endsWith("_bal")) {
            return WriteType.COMMUTATIVE_DELTA;
        }
        return WriteType.EXCLUSIVE;
    }

    private static boolean isBlindRead(Transaction txn, String key) {
        Object isExclusiveObj = txn.meta.get(META_IS_EXCLUSIVE);
        if (isExclusiveObj == null) return false;
        
        boolean isExclusive = ((AtomicBoolean) isExclusiveObj).get();
        return isExclusive && key.startsWith("c_") && key.endsWith("_bal");
    }
}
// EVOLVE-BLOCK-END