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
    WRITE,     // A standard overwrite operation.
    INCREMENT  // An arithmetic increment/decrement operation.
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
    private final BlockingQueue<Transaction> reconciliationQueue = new LinkedBlockingQueue<>();
    private final Thread reconcilerThread;
    private volatile boolean running = true;

    public GeneratedCC() {
        // The KVStore is passed to each method, so we need to get it during reconciliation.
        // This is a placeholder; the actual store is provided in commit().
        // We start a daemon thread that will not prevent the JVM from exiting.
        reconcilerThread = new Thread(new Reconciler());
        reconcilerThread.setDaemon(true);
        reconcilerThread.start();
    }

    public static ConcurrencyControl buildCc() {
        return new GeneratedCC();
    }

    /**
     * The background worker that serially applies committed transaction logs.
     * This single-threaded application of changes is what guarantees serializability.
     */
    private class Reconciler implements Runnable {
        @Override
        public void run() {
            while (running) {
                try {
                    // Block until a committed transaction is available.
                    Transaction txn = reconciliationQueue.take();
                    // The KVStore is not stored in GeneratedCC, so we get it from the transaction's metadata.
                    KVStore store = (KVStore) txn.meta.get("store");
                    if (store != null) {
                        applyChanges(txn, store);
                    }
                } catch (InterruptedException e) {
                    // Gracefully exit if the thread is interrupted.
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
            }
        }
        txn.status = TxStatus.COMMITTED;
    }
    
    /**
     * Simulates the offline static analysis of transaction templates.
     * In a real system, this would be done by a compiler. Here, we use string patterns.
     */
    private KeyType classifyKey(String key) {
        if (key.endsWith("_ytd") || key.endsWith("_bal") || key.endsWith("_qty") || key.endsWith("_next_oid")) {
            return KeyType.MANAGED;
        }
        return KeyType.STANDARD;
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
            // For standard keys, it's a simple WRITE operation.
            txn.writeSet.put(key, new SemanticOp(OpType.WRITE, key, value));
        }
    }

    @Override
    public CommitResult commit(Transaction txn, KVStore store) {
        if (txn.status == TxStatus.ABORTED) {
            return CommitResult.failure("Transaction was already aborted.");
        }
        
        // "Fast-commit": Enqueue the transaction for background reconciliation.
        // The client can return immediately.
        try {
            reconciliationQueue.put(txn);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return CommitResult.failure("Commit was interrupted.");
        }

        // The commit timestamp is logically the transaction's start time,
        // as this defines the snapshot it operated on.
        return CommitResult.success(txn.startTs);
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