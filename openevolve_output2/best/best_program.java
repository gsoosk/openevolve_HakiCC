package benchmark;


import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.*;

// --- Helper Classes for SHGL ---

/**
 * Defines the types of locks used in the SHGL protocol.
 * S: Shared (for reads)
 * X: Exclusive (for writes)
 * CU: Commutative Update (for read-modify-writes)
 */
enum LockMode {
    S, X, CU
}

/**
 * Represents a lock held by a transaction on a SemanticNode.
 */
class Lock {
    final int txid;
    final LockMode mode;

    Lock(int txid, LockMode mode) {
        this.txid = txid;
        this.mode = mode;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Lock lock = (Lock) o;
        return txid == lock.txid && mode == lock.mode;
    }

    @Override
    public int hashCode() {
        return Objects.hash(txid, mode);
    }
}

/**
 * Represents a node in the Semantic Graph (e.g., a Warehouse, District, Customer).
 * Each node acts as its own lock manager.
 */
class SemanticNode {
    private final String nodeId;
    private final ReentrantLock internalLock = new ReentrantLock();
    private final Condition condition = internalLock.newCondition();
    private final List<Lock> heldLocks = new ArrayList<>();

    // Lock compatibility matrix. [requesting][held]
    private static final boolean[][] COMPATIBILITY_MATRIX = {
        //         S      X      CU
        /* S */  { true,  false, false },
        /* X */  { false, false, false },
        /* CU */ { false, false, true  }  // CU can be compatible with other CUs for atomic operations (increment/decrement).
    };

    public SemanticNode(String nodeId) {
        this.nodeId = nodeId;
    }

    public boolean acquire(int txid, LockMode mode, long timeoutMillis) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        internalLock.lock();
        try {
            Lock existingLockForTx = null;
            Iterator<Lock> it = heldLocks.iterator();
            while (it.hasNext()) {
                Lock lock = it.next();
                if (lock.txid == txid) {
                    existingLockForTx = lock;
                    break;
                }
            }

            if (existingLockForTx != null) {
                if (existingLockForTx.mode.ordinal() >= mode.ordinal()) {
                    return true; // Already hold a strong enough lock
                }
                // Upgrade scenario: existingLockForTx.mode < mode.
                // Remove the old weaker lock before attempting to acquire the new, stronger one.
                heldLocks.remove(existingLockForTx);
            }

            // Now, try to acquire the new lock (or the stronger lock if it was an upgrade)
            while (!isCompatible(txid, mode)) {
                if (System.currentTimeMillis() >= deadline) {
                    // If we removed an old lock for upgrade, re-add it before failing.
                    if (existingLockForTx != null) {
                        heldLocks.add(existingLockForTx);
                    }
                    return false; // Timeout
                }
                if (!condition.await(deadline - System.currentTimeMillis(), TimeUnit.MILLISECONDS)) {
                    // If we removed an old lock for upgrade, re-add it before failing.
                    if (existingLockForTx != null) {
                        heldLocks.add(existingLockForTx);
                    }
                    return false; // Timeout
                }
            }
            heldLocks.add(new Lock(txid, mode));
            return true;
        } finally {
            internalLock.unlock();
        }
    }

    public void release(int txid) {
        internalLock.lock();
        try {
            heldLocks.removeIf(lock -> lock.txid == txid);
            condition.signalAll();
        } finally {
            internalLock.unlock();
        }
    }

    private boolean isCompatible(int txid, LockMode requestMode) {
        for (Lock heldLock : heldLocks) {
            if (heldLock.txid == txid) {
                continue;
            }
            if (!COMPATIBILITY_MATRIX[requestMode.ordinal()][heldLock.mode.ordinal()]) {
                return false;
            }
        }
        return true;
    }
}

/**
 * Manages the semantic graph and maps database keys to graph nodes and lock modes.
 */
class GraphManager {
    private static final GraphManager INSTANCE = new GraphManager();
    private final ConcurrentHashMap<String, SemanticNode> graph = new ConcurrentHashMap<>();

    // Removed unused Pattern declarations as key parsing now uses String.indexOf and substring.


    private GraphManager() {}

    public static GraphManager getInstance() {
        return INSTANCE;
    }

    private SemanticNode getNode(String nodeId) {
        return graph.computeIfAbsent(nodeId, SemanticNode::new);
    }

    static class AnalysisResult {
        final SemanticNode node;
        final LockMode mode;
        AnalysisResult(SemanticNode node, LockMode mode) {
            this.node = node;
            this.mode = mode;
        }
    }

    public AnalysisResult analyze(String key) {
        if (key.endsWith("_ytd") || key.endsWith("_bal") || key.endsWith("_qty") || key.endsWith("_next_oid")) {
            return new AnalysisResult(getNodeForKey(key), LockMode.CU);
        }
        if (key.startsWith("item_") && key.endsWith("_price")) {
            return new AnalysisResult(getNodeForKey(key), LockMode.S);
        }
        if (key.startsWith("o_")) {
             return new AnalysisResult(getNodeForKey(key), LockMode.X);
        }
        return new AnalysisResult(getNodeForKey(key), LockMode.X);
    }

    private SemanticNode getNodeForKey(String key) {
        int first_underscore, second_underscore, third_underscore;
        String p1, p2, p3;

        // Optimized parsing for CU keys using String.indexOf and substring for finer-grained semantic nodes
        if (key.endsWith("_ytd")) {
            if (key.startsWith("w_")) { // w_X_ytd
                first_underscore = key.indexOf('_');
                p1 = key.substring(first_underscore + 1, key.lastIndexOf('_'));
                return getNode("W-" + p1 + "-YTD");
            } else if (key.startsWith("d_")) { // d_X_Y_ytd
                first_underscore = key.indexOf('_');
                second_underscore = key.indexOf('_', first_underscore + 1);
                p1 = key.substring(first_underscore + 1, second_underscore);
                p2 = key.substring(second_underscore + 1, key.lastIndexOf('_'));
                return getNode("D-" + p1 + "-" + p2 + "-YTD");
            } else if (key.startsWith("c_")) { // c_X_Y_Z_ytd
                first_underscore = key.indexOf('_');
                second_underscore = key.indexOf('_', first_underscore + 1);
                third_underscore = key.indexOf('_', second_underscore + 1);
                p1 = key.substring(first_underscore + 1, second_underscore);
                p2 = key.substring(second_underscore + 1, third_underscore);
                p3 = key.substring(third_underscore + 1, key.lastIndexOf('_'));
                return getNode("C-" + p1 + "-" + p2 + "-" + p3 + "-YTD");
            }
        } else if (key.endsWith("_bal")) { // c_X_Y_Z_bal
            if (key.startsWith("c_")) {
                first_underscore = key.indexOf('_');
                second_underscore = key.indexOf('_', first_underscore + 1);
                third_underscore = key.indexOf('_', second_underscore + 1);
                p1 = key.substring(first_underscore + 1, second_underscore);
                p2 = key.substring(second_underscore + 1, third_underscore);
                p3 = key.substring(third_underscore + 1, key.lastIndexOf('_'));
                return getNode("C-" + p1 + "-" + p2 + "-" + p3 + "-BAL");
            }
        } else if (key.endsWith("_qty")) { // s_X_Y_qty
            if (key.startsWith("s_")) {
                first_underscore = key.indexOf('_');
                second_underscore = key.indexOf('_', first_underscore + 1);
                p1 = key.substring(first_underscore + 1, second_underscore);
                p2 = key.substring(second_underscore + 1, key.lastIndexOf('_'));
                return getNode("S-" + p1 + "-" + p2 + "-QTY");
            }
        } else if (key.endsWith("_next_oid")) { // d_X_Y_next_oid
            if (key.startsWith("d_")) {
                first_underscore = key.indexOf('_');
                second_underscore = key.indexOf('_', first_underscore + 1);
                p1 = key.substring(first_underscore + 1, second_underscore);
                p2 = key.substring(second_underscore + 1, key.lastIndexOf('_'));
                return getNode("D-" + p1 + "-" + p2 + "-NEXTOID");
            }
        }

        // Optimized parsing for general patterns using String.indexOf and substring
        if (key.startsWith("c_")) { // c_X_Y_Z (e.g., c_1_1_1_name, if such key existed)
            first_underscore = key.indexOf('_');
            second_underscore = key.indexOf('_', first_underscore + 1);
            third_underscore = key.indexOf('_', second_underscore + 1);
            p1 = key.substring(first_underscore + 1, second_underscore);
            p2 = key.substring(second_underscore + 1, third_underscore);
            p3 = key.substring(third_underscore + 1); // Remaining part after the third underscore
            return getNode("C-" + p1 + "-" + p2 + "-" + p3);
        } else if (key.startsWith("d_")) { // d_X_Y (e.g., d_1_1_name)
            first_underscore = key.indexOf('_');
            second_underscore = key.indexOf('_', first_underscore + 1);
            p1 = key.substring(first_underscore + 1, second_underscore);
            p2 = key.substring(second_underscore + 1); // Remaining part after the second underscore
            return getNode("D-" + p1 + "-" + p2);
        } else if (key.startsWith("s_")) { // s_X_Y (e.g., s_1_1_other_data)
            first_underscore = key.indexOf('_');
            second_underscore = key.indexOf('_', first_underscore + 1);
            p1 = key.substring(first_underscore + 1, second_underscore);
            p2 = key.substring(second_underscore + 1); // Remaining part after the second underscore
            return getNode("S-" + p1 + "-" + p2);
        } else if (key.startsWith("w_")) { // w_X (e.g., w_1_name)
            first_underscore = key.indexOf('_');
            p1 = key.substring(first_underscore + 1); // Remaining part after the first underscore
            return getNode("W-" + p1);
        } else if (key.startsWith("o_")) { // o_X_Y_Z_total
            first_underscore = key.indexOf('_');
            second_underscore = key.indexOf('_', first_underscore + 1);
            third_underscore = key.indexOf('_', second_underscore + 1);
            p1 = key.substring(first_underscore + 1, second_underscore);
            p2 = key.substring(second_underscore + 1, third_underscore);
            p3 = key.substring(third_underscore + 1, key.lastIndexOf('_')); // The Z part before "_total"
            return getNode("O-" + p1 + "-" + p2 + "-" + p3);
        } else if (key.startsWith("item_")) { // item_X_price
            first_underscore = key.indexOf('_');
            p1 = key.substring(first_underscore + 1, key.lastIndexOf('_')); // The X part before "_price"
            return getNode("I-" + p1);
        }
        return getNode(key); // Final fallback for any key not matched by specific or general patterns
    }
}


class GeneratedCC extends ConcurrencyControl {
    private final AtomicInteger txidCounter = new AtomicInteger(0);
    private final GraphManager graphManager = GraphManager.getInstance();
    // CORRECTED: Reduced timeout to break deadlocks faster and avoid stalled threads.
    private static final long LOCK_TIMEOUT_MS = 100;
    
    private static final Object NULL_PLACEHOLDER = new Object();

    public static ConcurrencyControl buildCc() {
        return new GeneratedCC();
    }

    @Override
    public Transaction begin(KVStore store) {
        int txid = txidCounter.incrementAndGet();
        Transaction txn = new Transaction(txid, 0);
        txn.meta.put("locked_nodes", new HashSet<SemanticNode>());
        return txn;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object read(Transaction txn, String key, KVStore store) {
        if (txn.status != TxStatus.ACTIVE) return null;

        if (txn.writeSet.containsKey(key)) {
            Object value = txn.writeSet.get(key);
            return value == NULL_PLACEHOLDER ? null : value;
        }

        GraphManager.AnalysisResult analysis = graphManager.analyze(key);
        try {
            if (!analysis.node.acquire(txn.txid, analysis.mode, LOCK_TIMEOUT_MS)) {
                txn.status = TxStatus.ABORTED;
                return null;
            }
            ((Set<SemanticNode>) txn.meta.get("locked_nodes")).add(analysis.node);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            txn.status = TxStatus.ABORTED;
            return null;
        }

        Object value = store.get(key);
        txn.readSet.put(key, value == null ? NULL_PLACEHOLDER : value);
        return value;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void write(Transaction txn, String key, Object value, KVStore store) {
        if (txn.status != TxStatus.ACTIVE) return;

        GraphManager.AnalysisResult analysis = graphManager.analyze(key);
        LockMode requestedMode = analysis.mode;

        // If the key is designated for CU, but the write operation is non-commutative (delete or non-integer update),
        // then escalate the lock request to Exclusive (X) to ensure serializability.
        // This check is applied *before* acquiring the lock.
        if (requestedMode == LockMode.CU) {
            if (value == NULL_PLACEHOLDER || !(value instanceof Integer)) {
                requestedMode = LockMode.X;
            }
        }

        try {
            // Use the potentially escalated requestedMode
            if (!analysis.node.acquire(txn.txid, requestedMode, LOCK_TIMEOUT_MS)) {
                txn.status = TxStatus.ABORTED;
                return;
            }
            ((Set<SemanticNode>) txn.meta.get("locked_nodes")).add(analysis.node);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            txn.status = TxStatus.ABORTED;
            return;
        }

        txn.writeSet.put(key, value == null ? NULL_PLACEHOLDER : value);
    }

    @Override
    public CommitResult commit(Transaction txn, KVStore store) {
        if (txn.status == TxStatus.ABORTED) {
            rollback(txn, store);
            return CommitResult.failure("Transaction was aborted due to conflict or timeout.");
        }
        if (txn.status == TxStatus.COMMITTED) {
            return CommitResult.failure("Transaction already committed.");
        }

        if (!txn.writeSet.isEmpty()) {
            Map<String, Object> updatesForUpdateAll = new HashMap<>();
            for (Map.Entry<String, Object> entry : txn.writeSet.entrySet()) {
                String key = entry.getKey();
                Object bufferedValue = entry.getValue();

                GraphManager.AnalysisResult analysis = graphManager.analyze(key);

                // Attempt to use atomic increment/decrement for CU-locked keys
                if (analysis.mode == LockMode.CU &&
                    bufferedValue instanceof Integer && // Check if the buffered value is an Integer
                    txn.readSet.containsKey(key)) { // Check if the key was initially read by this transaction
                    
                    Object originalReadValueObj = txn.readSet.get(key);
                    if (originalReadValueObj instanceof Integer) { // Check if the original value was an Integer
                        Integer originalReadValue = (Integer) originalReadValueObj;
                        Integer newIntValue = (Integer) bufferedValue;
                        int delta = newIntValue - originalReadValue;

                        if (delta > 0) {
                            store.increment(key, delta);
                        } else if (delta < 0) {
                            store.decrement(key, -delta);
                        }
                        // If delta == 0, no change needed.
                        continue; // This CU key has been processed by atomic operation
                    }
                }

                // For non-CU keys, or CU keys that couldn't be handled by increment/decrement (e.g., non-numeric), or deletions
                if (bufferedValue == NULL_PLACEHOLDER) {
                    store.delete(key);
                } else {
                    // For inserts/updates that are not handled by increment/decrement
                    updatesForUpdateAll.put(key, bufferedValue);
                }
            }
            if (!updatesForUpdateAll.isEmpty()) {
                store.updateAll(updatesForUpdateAll);
            }
        }

        releaseAllLocks(txn);
        txn.status = TxStatus.COMMITTED;
        return CommitResult.success(0);
    }

    @Override
    public void rollback(Transaction txn, KVStore store) {
        if (txn.status != TxStatus.COMMITTED) {
            releaseAllLocks(txn);
            txn.status = TxStatus.ABORTED;
        }
    }

    @SuppressWarnings("unchecked")
    private void releaseAllLocks(Transaction txn) {
        Set<SemanticNode> lockedNodes = (Set<SemanticNode>) txn.meta.get("locked_nodes");
        if (lockedNodes != null) {
            for (SemanticNode node : lockedNodes) {
                node.release(txn.txid);
            }
            lockedNodes.clear();
        }
    }
}
