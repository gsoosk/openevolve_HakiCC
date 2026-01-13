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
        /* CU */ { false, false, false  } // CORRECTED: CU must be exclusive with other CUs to prevent lost updates.
    };

    public SemanticNode(String nodeId) {
        this.nodeId = nodeId;
    }

    public boolean acquire(int txid, LockMode mode, long timeoutMillis) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        internalLock.lock();
        try {
            for (Lock lock : heldLocks) {
                if (lock.txid == txid) {
                    if (lock.mode.ordinal() >= mode.ordinal()) { // CU > X > S
                        return true; // Already hold a strong enough lock
                    }
                    // Lock upgrade would be complex, so we pessimistically acquire strong locks from the start.
                }
            }

            while (!isCompatible(txid, mode)) {
                if (System.currentTimeMillis() >= deadline) {
                    return false; // Timeout
                }
                if (!condition.await(deadline - System.currentTimeMillis(), TimeUnit.MILLISECONDS)) {
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

    private static final Pattern WAREHOUSE_PATTERN = Pattern.compile("^w_(\\d+)_.*");
    private static final Pattern DISTRICT_PATTERN = Pattern.compile("^d_(\\d+)_(\\d+)_.*");
    private static final Pattern CUSTOMER_PATTERN = Pattern.compile("^c_(\\d+)_(\\d+)_(\\d+)_.*");
    private static final Pattern ITEM_PATTERN = Pattern.compile("^item_(\\d+)_.*");
    private static final Pattern STOCK_PATTERN = Pattern.compile("^s_(\\d+)_(\\d+)_.*");
    private static final Pattern ORDER_PATTERN = Pattern.compile("^o_(\\d+)_(\\d+)_(\\d+)_.*");


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
        Matcher m;
        m = CUSTOMER_PATTERN.matcher(key); if (m.matches()) return getNode("C-" + m.group(1) + "-" + m.group(2) + "-" + m.group(3));
        m = DISTRICT_PATTERN.matcher(key); if (m.matches()) return getNode("D-" + m.group(1) + "-" + m.group(2));
        m = STOCK_PATTERN.matcher(key); if (m.matches()) return getNode("S-" + m.group(1) + "-" + m.group(2));
        m = WAREHOUSE_PATTERN.matcher(key); if (m.matches()) return getNode("W-" + m.group(1));
        m = ORDER_PATTERN.matcher(key); if (m.matches()) return getNode("O-" + m.group(1) + "-" + m.group(2) + "-" + m.group(3));
        m = ITEM_PATTERN.matcher(key); if (m.matches()) return getNode("I-" + m.group(1));
        return getNode(key);
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
        try {
            if (!analysis.node.acquire(txn.txid, analysis.mode, LOCK_TIMEOUT_MS)) {
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
            Map<String, Object> writesToApply = new HashMap<>();
            for (Map.Entry<String, Object> entry : txn.writeSet.entrySet()) {
                Object value = entry.getValue();
                writesToApply.put(entry.getKey(), value == NULL_PLACEHOLDER ? null : value);
            }
            store.updateAll(writesToApply);
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
