package benchmark;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

// --- Helper Enums and Classes for PACE Protocol ---

/**
 * Defines the type of a buffered operation in the PACE protocol.
 */
enum PaceOperationType {
    ATOMIC_ADD,
    SET
}

/**
 * Represents a single buffered operation within a transaction's op_log.
 */
class PaceOperation {
    public final PaceOperationType type;
    public final String key;
    public final Object value; // Represents delta for ATOMIC_ADD, final value for SET

    public PaceOperation(PaceOperationType type, String key, Object value) {
        this.type = type;
        this.key = key;
        this.value = value;
    }
}

/**
 * Defines the semantic type of a key based on static analysis of the TPC-C workload.
 */
enum KeyType {
    COMMUTATIVE,
    INSERT_ONLY,
    OVERWRITE
}


/**
 * Pre-Analyzed Commutative Execution (PACE) Concurrency Control Protocol.
 * This version implements a correct, strict OCC validation phase to ensure serializability.
 */
class GeneratedCC extends ConcurrencyControl {

    // --- Global State for the PACE Protocol ---

    private static final AtomicInteger transactionIdCounter = new AtomicInteger(0);
    private static final AtomicInteger globalTimestamp = new AtomicInteger(0);
    private static final ReentrantLock commitLock = new ReentrantLock();
    private static final ConcurrentHashMap<String, AtomicInteger> keyVersionMap = new ConcurrentHashMap<>();
    private static final Object NULL_SENTINEL = new Object();

    // --- Static Analysis (Pre-computed) ---

    private static final Pattern COMMUTATIVE_PATTERN = Pattern.compile("^(w_\\d+_ytd|d_\\d+_\\d+_ytd|d_\\d+_\\d+_next_oid|c_\\d+_\\d+_\\d+_bal|c_\\d+_\\d+_\\d+_ytd|s_\\d+_\\d+_qty)$");
    private static final Pattern INSERT_ONLY_PATTERN = Pattern.compile("^o_\\d+_\\d+_\\d+_total$");

    private static KeyType getOperationType(String key) {
        if (COMMUTATIVE_PATTERN.matcher(key).matches()) {
            return KeyType.COMMUTATIVE;
        }
        if (INSERT_ONLY_PATTERN.matcher(key).matches()) {
            return KeyType.INSERT_ONLY;
        }
        return KeyType.OVERWRITE;
    }

    // --- Metadata Keys for Transaction.meta ---

    private static final String META_OP_LOG = "op_log";
    private static final String META_READ_VERSIONS = "read_versions";

    public static ConcurrencyControl buildCc() {
        return new GeneratedCC();
    }

    @Override
    public Transaction begin(KVStore store) {
        int txid = transactionIdCounter.incrementAndGet();
        int startTs = globalTimestamp.incrementAndGet();
        Transaction txn = new Transaction(txid, startTs);

        txn.meta.put(META_OP_LOG, new ArrayList<PaceOperation>());
        txn.meta.put(META_READ_VERSIONS, new ConcurrentHashMap<String, Integer>());

        return txn;
    }

    @Override
    public Object read(Transaction txn, String key, KVStore store) {
        if (txn.status != TxStatus.ACTIVE) {
            return null;
        }

        if (txn.writeSet.containsKey(key)) {
            Object writtenValue = txn.writeSet.get(key);
            return (writtenValue == NULL_SENTINEL) ? null : writtenValue;
        }

        // FIX: Read version FIRST to prevent race condition with concurrent commits.
        // This ensures that if a commit happens between reading the version and the data,
        // the validation will correctly fail.
        @SuppressWarnings("unchecked")
        Map<String, Integer> readVersions = (Map<String, Integer>) txn.meta.get(META_READ_VERSIONS);
        AtomicInteger version = keyVersionMap.get(key);
        readVersions.put(key, version != null ? version.get() : 0);

        // Read data SECOND.
        Object value = store.get(key);
        if (value instanceof AtomicLong) {
            value = ((AtomicLong) value).get();
        }

        Object valueToStore = (value == null) ? NULL_SENTINEL : value;
        txn.readSet.put(key, valueToStore);

        return value;
    }

    @Override
    public void write(Transaction txn, String key, Object value, KVStore store) {
        if (txn.status != TxStatus.ACTIVE) {
            return;
        }

        Object valueToStore = (value == null) ? NULL_SENTINEL : value;
        txn.writeSet.put(key, valueToStore);

        KeyType type = getOperationType(key);
        @SuppressWarnings("unchecked")
        List<PaceOperation> opLog = (List<PaceOperation>) txn.meta.get(META_OP_LOG);

        if (type == KeyType.COMMUTATIVE) {
            Object oldValueObj = txn.readSet.get(key);
            
            long oldValue = 0;
            if (oldValueObj != null && oldValueObj != NULL_SENTINEL) {
                oldValue = ((Number) oldValueObj).longValue();
            }

            long newValue = ((Number) value).longValue();
            long delta = newValue - oldValue;
            opLog.add(new PaceOperation(PaceOperationType.ATOMIC_ADD, key, delta));
        } else {
            opLog.add(new PaceOperation(PaceOperationType.SET, key, value));
        }
    }

    @Override
    public CommitResult commit(Transaction txn, KVStore store) {
        if (txn.status != TxStatus.ACTIVE) {
            return CommitResult.failure("Transaction already " + txn.status);
        }

        @SuppressWarnings("unchecked")
        List<PaceOperation> opLog = (List<PaceOperation>) txn.meta.get(META_OP_LOG);

        commitLock.lock();
        try {
            // --- 1. Validation Phase (Strict OCC) ---
            @SuppressWarnings("unchecked")
            Map<String, Integer> readVersions = (Map<String, Integer>) txn.meta.get(META_READ_VERSIONS);

            for (Map.Entry<String, Integer> entry : readVersions.entrySet()) {
                String key = entry.getKey();
                int readVersion = entry.getValue();

                AtomicInteger currentVersion = keyVersionMap.get(key);
                int currentVersionVal = (currentVersion != null) ? currentVersion.get() : 0;

                if (currentVersionVal > readVersion) {
                    txn.status = TxStatus.ABORTED;
                    return CommitResult.failure("Validation failed on key: " + key);
                }
            }

            // --- 2. Write Phase ---
            int commitTs = globalTimestamp.incrementAndGet();

            for (PaceOperation op : opLog) {
                switch (op.type) {
                    case ATOMIC_ADD:
                        store.increment(op.key, (long) op.value);
                        break;
                    case SET:
                        store.update(op.key, op.value);
                        break;
                }
            }
            
            // --- 3. Version Update Phase ---
            for (PaceOperation op : opLog) {
                 keyVersionMap.computeIfAbsent(op.key, k -> new AtomicInteger(0)).set(commitTs);
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
        }
        txn.readSet.clear();
        txn.writeSet.clear();
        txn.meta.clear();
    }
}
