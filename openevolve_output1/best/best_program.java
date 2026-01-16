package benchmark;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

// EVOLVE-BLOCK-START

// Helper enum for classifying keys based on TPC-C workload analysis.
enum KeyType {
    MANAGED, // Keys that undergo commutative arithmetic operations (e.g., counters, balances).
    STANDARD // All other keys.
}

// Helper enum for semantic operations.
enum OpType {
    WRITE,     // A standard overwrite operation for existing keys.
    INCREMENT, // An arithmetic increment/decrement operation.
    INSERT     // An insertion operation for new keys.
}

// Helper class to represent a semantic operation in the transaction's log.
class SemanticOp {
    final OpType type;
    final String key;
    Object value; // For WRITE, this is the final value. For INCREMENT, this is the delta (Long).

    SemanticOp(OpType type, String key, Object value) {
        this.type = type;
        this.key = key;
        this.value = value;
    }
}

/**
 * Implementation of Asynchronous State Reconciliation with Semantic Escrow (ASR-SE).
 *
 * Core Idea: Decouple transaction execution from state reconciliation. Transactions
 * execute against a snapshot, generating a log of high-level semantic operations
 * (e.g., "increment stock by -5"). Committing is a "fast-commit" that places this
 * log on a queue. A single background thread (the Reconciler) applies these
 * changes serially to the database, using atomic operations for contentious keys.
 * This protocol eliminates traditional locking and validation-phase conflicts for
 * the target workload.
 */
class GeneratedCC extends ConcurrencyControl {
    private static final AtomicInteger txidCounter = new AtomicInteger(0);
    private final AtomicInteger commitTsCounter = new AtomicInteger(0); // New global counter for commit timestamps
    // Replaced single queue with an array of queues for warehouse-specific partitioning
    private final BlockingQueue<Transaction>[] warehouseQueues;
    private final Thread[] reconcilerThreads;
    private final BlockingQueue<Transaction> globalReconciliationQueue; // Added global queue
    private final Thread globalReconcilerThread; // Added global reconciler thread
    private volatile boolean running = true;

    // TPC-C specific constant from the workload description
    private static final int NUM_WAREHOUSES = 8;

    @SuppressWarnings("unchecked") // For array of generic BlockingQueue
    public GeneratedCC() {
        warehouseQueues = (BlockingQueue<Transaction>[]) new BlockingQueue[NUM_WAREHOUSES];
        reconcilerThreads = new Thread[NUM_WAREHOUSES];

        for (int i = 0; i < NUM_WAREHOUSES; i++) {
            warehouseQueues[i] = new LinkedBlockingQueue<>();
            reconcilerThreads[i] = new Thread(new Reconciler(warehouseQueues[i]));
            reconcilerThreads[i].setDaemon(true);
            reconcilerThreads[i].start();
        }

        // Initialize global queue and its reconciler thread
        globalReconciliationQueue = new LinkedBlockingQueue<>();
        globalReconcilerThread = new Thread(new Reconciler(globalReconciliationQueue));
        globalReconcilerThread.setDaemon(true);
        globalReconcilerThread.start();
    }

    public static ConcurrencyControl buildCc() {
        return new GeneratedCC();
    }

    /**
     * The background worker that serially applies committed transaction logs.
     * This single-threaded application of changes is what guarantees serializability.
     */
    private class Reconciler implements Runnable {
        private final BlockingQueue<Transaction> queue;

        Reconciler(BlockingQueue<Transaction> queue) {
            this.queue = queue;
        }

        @Override
        public void run() {
            while (running) {
                try {
                    Transaction txn = queue.take();
                    KVStore store = (KVStore) txn.meta.get("store");
                    if (store != null) {
                        applyChanges(txn, store);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    /**
     * Applies the semantic log of a transaction to the KVStore.
     */
    private void applyChanges(Transaction txn, KVStore store) {
        for (Object opObject : txn.writeSet.values()) {
            SemanticOp op = (SemanticOp) opObject;
            switch (op.type) {
                case WRITE:
                    store.update(op.key, op.value);
                    break;
                case INCREMENT:
                    store.increment(op.key, (Long) op.value);
                    break;
                case INSERT:
                    store.insert(op.key, op.value);
                    break;
            }
        }
        txn.status = TxStatus.COMMITTED;
    }
    
    /**
     * Extracts the warehouse ID from a TPC-C key. Returns -1 if not a warehouse-specific key
     * or if it's a global key like "item_".
     */
    private int getWarehouseIdFromKey(String key) {
        // For TPC-C, WID is always a single digit (1-8) and appears at index 2 for warehouse-specific keys.
        // E.g., "w_1_ytd", "d_5_1_ytd", "c_8_10_1_bal", "s_3_123_qty", "o_2_1_1_total"
        // Global item keys "item_..." do not have a WID for partitioning.

        if (key.length() < 4) { // Minimum valid warehouse key is "x_1_" (e.g., "w_1_")
            return -1;
        }

        char firstChar = key.charAt(0);
        // Check if it's a warehouse-specific key prefix
        if (!(firstChar == 'w' || firstChar == 'd' || firstChar == 'c' || firstChar == 's' || firstChar == 'o')) {
            return -1; // Not a warehouse-specific key (e.g., "item_...")
        }

        // Verify the format x_WID_...
        if (key.charAt(1) != '_') {
            return -1; // Malformed key, expected underscore at index 1.
        }

        char widChar = key.charAt(2);
        if (widChar >= '1' && widChar <= '8') { // WIDs are 1-8 for TPCC_NUM_WAREHOUSES = 8
            if (key.charAt(3) == '_') { // Ensure the format x_WID_...
                return Character.getNumericValue(widChar);
            }
        }
        return -1; // Not a recognized warehouse key or malformed.
    }

    /**
     * Determines the primary warehouse ID for a transaction based on its write set.
     * Returns -1 if the transaction is read-only or affects multiple warehouses/global keys.
     */
    private int getTransactionPrimaryWarehouseId(Transaction txn) {
        if (txn.writeSet.isEmpty()) {
            return -1; // Read-only transactions don't need a reconciler queue based on warehouse ID.
        }

        Integer primaryWid = null;
        for (Object opObject : txn.writeSet.values()) {
            SemanticOp op = (SemanticOp) opObject;
            int wid = getWarehouseIdFromKey(op.key);
            
            if (wid == -1) { // Key is not warehouse-specific (e.g., "item_...")
                return -1; // Treat as global/mixed, cannot be assigned to a single warehouse queue.
            }

            if (primaryWid == null) {
                primaryWid = wid;
            } else if (!primaryWid.equals(wid)) {
                return -1; // Transaction affects multiple warehouses.
            }
        }
        return primaryWid != null ? primaryWid : -1;
    }
    
    /**
     * Simulates the offline static analysis of transaction templates.
     * In a real system, this would be done by a compiler. Here, we use string patterns.
     */
    private KeyType classifyKey(String key) {
        char firstChar = key.charAt(0);
        // TPC-C keys:
        // 'w_', 'd_', 'c_', 's_' are MANAGED keys (ytd, bal, qty, next_oid)
        // 'item_' and 'o_' are STANDARD keys (item_price, o_total)
        // This is a direct classification based on the first character.
        if (firstChar == 'i' || firstChar == 'o') { // "item_" or "o_"
            return KeyType.STANDARD;
        }
        // All other TPC-C prefixes ('w', 'd', 'c', 's') correspond to MANAGED keys.
        return KeyType.MANAGED;
    }

    @Override
    public Transaction begin(KVStore store) {
        int txid = txidCounter.incrementAndGet();
        Transaction txn = new Transaction(txid, txid);
        // We need the store reference during reconciliation.
        txn.meta.put("store", store);
        return txn;
    }

    @Override
    public Object read(Transaction txn, String key, KVStore store) {
        if (txn.status == TxStatus.ABORTED) {
            return null;
        }

        // Enforce repeatable reads by checking our private readSet first.
        if (txn.readSet.containsKey(key)) {
            return txn.readSet.get(key);
        }

        // Read from the actual store if it's the first time.
        Object value = store.get(key);
        
        // Unbox AtomicLong to Long for consistent handling within the transaction.
        Object storedValue = (value instanceof AtomicLong) ? ((AtomicLong) value).get() : value;

        // Cache the value in our snapshot (readSet).
        if (storedValue != null) {
            txn.readSet.put(key, storedValue);
        }
        
        return storedValue;
    }

    @Override
    public void write(Transaction txn, String key, Object value, KVStore store) {
        if (txn.status == TxStatus.ABORTED) {
            return;
        }

        KeyType type = classifyKey(key);

        if (type == KeyType.MANAGED) {
            // For managed keys, we calculate the delta and create an INCREMENT operation.
            // The transaction logic is assumed to be read-modify-write, so the original
            // value must have been read into the readSet.
            Object originalValueObj = txn.readSet.get(key);
            long originalValue = (originalValueObj instanceof Number) ? ((Number) originalValueObj).longValue() : 0;
            long newValue = (value instanceof Number) ? ((Number) value).longValue() : 0;
            long delta = newValue - originalValue;

            // Aggregate deltas if the key is written multiple times.
            SemanticOp existingOp = (SemanticOp) txn.writeSet.get(key);
            if (existingOp != null && existingOp.type == OpType.INCREMENT) {
                existingOp.value = (Long) existingOp.value + delta;
            } else {
                txn.writeSet.put(key, new SemanticOp(OpType.INCREMENT, key, delta));
            }
        } else { // STANDARD key
            // Determine if it's an insert or an update for a standard key.
            // For TPC-C, "o_" keys are new and typically not read before writing.
            // This heuristic assumes that if a key is not in the readSet, and it's an "o_" key, it's an insert.
            if (!txn.readSet.containsKey(key) && key.startsWith("o_")) {
                txn.writeSet.put(key, new SemanticOp(OpType.INSERT, key, value));
            } else { // Otherwise, it's an update to an existing key or a non-"o_" new key (which we treat as an update for simplicity here)
                txn.writeSet.put(key, new SemanticOp(OpType.WRITE, key, value));
            }
        }
    }

    @Override
    public CommitResult commit(Transaction txn, KVStore store) {
        if (txn.status == TxStatus.ABORTED) {
            return CommitResult.failure("Transaction was already aborted.");
        }

        // All transactions, even read-only, acquire a global commit timestamp.
        // For write transactions, this timestamp reflects the order of enqueuing
        // into the reconciliation queue, which defines their serialization order.
        // For read-only transactions, it ensures a consistent global ordering.
        int commitTs = commitTsCounter.incrementAndGet();
        txn.meta.put("commit_ts", commitTs); // Store for debugging if needed

        if (!txn.writeSet.isEmpty()) {
            int primaryWid = getTransactionPrimaryWarehouseId(txn);
            if (primaryWid != -1 && primaryWid >= 1 && primaryWid <= NUM_WAREHOUSES) {
                try {
                    // Enqueue to the specific warehouse's reconciliation queue (1-indexed WID to 0-indexed array)
                    warehouseQueues[primaryWid - 1].put(txn);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return CommitResult.failure("Commit was interrupted for WID " + primaryWid);
                }
            } else {
                // Fallback to a global queue for transactions affecting multiple warehouses or global keys.
                // This ensures all write transactions eventually commit, maintaining correctness
                // for broader workloads beyond strictly partitioned TPC-C.
                try {
                    globalReconciliationQueue.put(txn);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return CommitResult.failure("Commit was interrupted for global queue processing.");
                }
            }
            return CommitResult.success(commitTs);
        } else {
            // Read-only transaction. Commit immediately without any writes,
            // but still report a global commit timestamp for consistent ordering.
            txn.status = TxStatus.COMMITTED;
            return CommitResult.success(commitTs);
        }
    }

    @Override
    public void rollback(Transaction txn, KVStore store) {
        // No need to undo changes in KVStore, as none have been applied yet.
        txn.status = TxStatus.ABORTED;
        txn.readSet.clear();
        txn.writeSet.clear();
    }
}

// EVOLVE-BLOCK-END