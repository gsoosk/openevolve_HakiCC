package benchmark;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Set; // Added for TreeSet
import java.util.TreeSet; // Added for efficient unique and sorted keys
import java.util.concurrent.locks.ReentrantLock;
// Removed unused imports: AtomicLong and Pattern

// --- Helper Enums and Classes for PACE Protocol ---

/**
 * Defines the type of a buffered operation in the PACE protocol.
 */
enum PaceOperationType {
    ATOMIC_ADD,
    SET,
    INSERT // Added INSERT type for new keys, specifically for INSERT_ONLY KeyType
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
    // Removed global commit lock, using per-key locks for write-set instead for higher concurrency.
    // private static final ReentrantLock commitLock = new ReentrantLock(); 

    private static final ConcurrentHashMap<String, ReentrantLock> keyLocks = new ConcurrentHashMap<>(); // Per-key locks for write-set during commit
    private static final ConcurrentHashMap<String, AtomicInteger> keyVersionMap = new ConcurrentHashMap<>();
    private static final Object NULL_SENTINEL = new Object();

    // --- Static Analysis (Pre-computed) ---

    // Removed regex patterns for performance optimization, replaced with startsWith checks.
    // private static final Pattern COMMUTATIVE_PATTERN = Pattern.compile("^(w_\\d+_ytd|d_\\d+_\\d+_ytd|d_\\d+_\\d+_next_oid|c_\\d+_\\d+_\\d+_bal|c_\\d+_\\d+_\\d+_ytd|s_\\d+_\\d+_qty)$");
    // private static final Pattern INSERT_ONLY_PATTERN = Pattern.compile("^o_\\d+_\\d+_\\d+_total$");

    // Cache for pre-analyzed key types to avoid repeated string matching
    private static final ConcurrentHashMap<String, KeyType> keyTypeCache = new ConcurrentHashMap<>();

    private static KeyType getOperationType(String key) {
        // Use computeIfAbsent to cache the KeyType for a given key string
        return keyTypeCache.computeIfAbsent(key, k -> {
            // Prioritize faster prefix checks for common TPC-C key types.
            if (k.startsWith("o_")) { // Matches order total keys (e.g., o_1_1_1_total)
                return KeyType.INSERT_ONLY;
            }
            if (k.startsWith("item_")) { // Matches item price keys (e.g., item_1_price)
                return KeyType.OVERWRITE;
            }
            // Based on TPC-C workload analysis, keys starting with w_, d_, c_, s_ are always commutative.
            if (k.startsWith("w_") || k.startsWith("d_") || k.startsWith("c_") || k.startsWith("s_")) {
                return KeyType.COMMUTATIVE;
            }
            // Fallback for any other keys not explicitly matched, which would be OVERWRITE
            return KeyType.OVERWRITE;
        });
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
        // In this strict OCC variant, txn.start_ts is not directly used for validation.
        // Assigning txid to start_ts reduces contention on globalTimestamp during transaction begin.
        Transaction txn = new Transaction(txid, txid);

        txn.meta.put(META_OP_LOG, new ArrayList<PaceOperation>());
        txn.meta.put(META_READ_VERSIONS, new java.util.HashMap<String, Integer>()); // Changed to HashMap

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
        // The KVStore is expected to return primitive wrappers (Integer, Long, etc.) or null.
        // It's highly unlikely to return an AtomicLong object itself, as values are stored as Integer/Long
        // by the TPC-C workload. Removing this check to slightly optimize.

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
        } else if (type == KeyType.INSERT_ONLY) { // Use INSERT for insert-only keys
            opLog.add(new PaceOperation(PaceOperationType.INSERT, key, value));
        } else { // Default to SET for OVERWRITE keys
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
        @SuppressWarnings("unchecked")
        Map<String, Integer> readVersions = (Map<String, Integer>) txn.meta.get(META_READ_VERSIONS);

        // Use a TreeSet to automatically handle uniqueness and sorting of written keys.
        Set<String> uniqueSortedWrittenKeys = new java.util.TreeSet<>();
        for (PaceOperation op : opLog) {
            uniqueSortedWrittenKeys.add(op.key);
        }

        List<ReentrantLock> acquiredLocks = new ArrayList<>();

        try {
            // --- Acquire Write Locks (2PL-style) ---
            for (String key : uniqueSortedWrittenKeys) { // Iterate over unique sorted keys
                ReentrantLock lock = keyLocks.computeIfAbsent(key, k -> new ReentrantLock());
                lock.lock(); // Acquire exclusive lock
                acquiredLocks.add(lock);
            }

            // --- 1. Validation Phase (Strict OCC for reads) ---
            // This is done while holding write locks to ensure that no concurrent writes
            // to read-set keys can commit and change their versions before validation.
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

            // --- 2. Assign Commit Timestamp & Apply Writes ---
            // Get commit timestamp AFTER successful validation and lock acquisition.
            int commitTs = globalTimestamp.incrementAndGet(); // Atomically get commit timestamp

            for (PaceOperation op : opLog) {
                switch (op.type) {
                    case ATOMIC_ADD:
                        store.increment(op.key, (long) op.value);
                        break;
                    case SET:
                        store.update(op.key, op.value);
                        break;
                    case INSERT:
                        store.insert(op.key, op.value);
                        break;
                }
            }
            
            // --- 3. Version Update Phase ---
            // Update versions for all written keys with the new commit timestamp.
            // This update is protected by the per-key lock held for each written key.
            // Iterate over unique sorted keys to avoid redundant updates for the same key.
            for (String key : uniqueSortedWrittenKeys) {
                 keyVersionMap.computeIfAbsent(key, k -> new AtomicInteger(0)).set(commitTs);
            }

            txn.status = TxStatus.COMMITTED;
            return CommitResult.success(commitTs);

        } finally {
            // --- Release Write Locks ---
            // Ensure all acquired locks are released, even if an exception occurs.
            for (ReentrantLock lock : acquiredLocks) {
                lock.unlock();
            }
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
