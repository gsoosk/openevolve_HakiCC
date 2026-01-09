package benchmark;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


enum TxStatus {
    ACTIVE,
    COMMITTED,
    ABORTED
}

/**
 * Per-transaction state shared with the CC algorithm.
 * Uses ConcurrentHashMap for thread-safe read/write set operations.
 */
class Transaction {
    public final int txid;  // This should be unique for each transaction
    public final int startTs;
    public volatile TxStatus status = TxStatus.ACTIVE;
    public final ConcurrentHashMap<String, Object> readSet = new ConcurrentHashMap<>();    // key -> value read
    public final ConcurrentHashMap<String, Object> writeSet = new ConcurrentHashMap<>();   // key -> intended value
    public final ConcurrentHashMap<String, Object> meta = new ConcurrentHashMap<>();       // free-form for CC's internal use

    public Transaction(int txid, int startTs) {
        this.txid = txid;
        this.startTs = startTs;
    }
}

/**
 * Result of a commit operation.
 */
class CommitResult {
    public final boolean ok;
    public final String reason;
    public final Integer commitTs;  // CC should set commit_ts if it applies writes. May be null on abort.

    public CommitResult(boolean ok) {
        this(ok, null, null);
    }

    public CommitResult(boolean ok, String reason) {
        this(ok, reason, null);
    }

    public CommitResult(boolean ok, String reason, Integer commitTs) {
        this.ok = ok;
        this.reason = reason;
        this.commitTs = commitTs;
    }

    public static CommitResult success(int commitTs) {
        return new CommitResult(true, null, commitTs);
    }

    public static CommitResult failure(String reason) {
        return new CommitResult(false, reason, null);
    }
}

/**
 * CC Plugin Interface.
 * This is the core class that must be implemented to define a concurrency control mechanism.
 * It manages transaction visibility and conflict resolution through the methods defined below.
 * Implementations cannot modify the underlying storage system.
 */
abstract class ConcurrencyControl {
    
    /**
     * Initialize and return a new Transaction object, including any necessary metadata.
     */
    public abstract Transaction begin(KVStore store);

    /**
     * Retrieve the value accessible to the transaction, potentially using transaction-specific data.
     * If the transaction is aborted, return null.
     */
    public abstract Object read(Transaction txn, String key, KVStore store);

    /**
     * Record a write operation intent. Update the transaction's write set and any relevant metadata/transaction state.
     * If the transaction is aborted, do nothing.
     */
    public abstract void write(Transaction txn, String key, Object value, KVStore store);

    /**
     * Finalize the transaction. If successful, apply changes to the storage and return a successful CommitResult.
     * If not, return a CommitResult indicating failure and the reason.
     * If the transaction is aborted, return a CommitResult indicating failure and the reason.
     */
    public abstract CommitResult commit(Transaction txn, KVStore store);

    /**
     * Undo any changes and release resources associated with the transaction.
     */
    public abstract void rollback(Transaction txn, KVStore store);
}
