package benchmark;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.*;
import java.util.stream.Collectors;

/**
 * Evaluator for the concurrency control implementation.
 *
 * This evaluator:
 *   - Loads the candidate concurrency control implementation
 *   - Constructs a CC instance via buildCc()
 *   - Runs several concurrent workloads and builds a precedence graph from the
 *     actual get/put operations hitting the KVStore
 *   - Checks for conflict-serializability (no cycles) and for lack of stalls
 *   - Reports metrics such as correctness and throughput in an EvaluationResult
 */
public class Evaluator {

    // ---------- Operation record ----------
    
    static class Op {
        final int seq;
        final int txid;
        final char kind;  // 'R' = Read, 'W' = Write, 'I' = Increment, 'D' = Decrement
        final String key;

        Op(int seq, int txid, char kind, String key) {
            this.seq = seq;
            this.txid = txid;
            this.kind = kind;
            this.key = key;
        }
    }

    // ---------- MockKVStore ----------
    
    /**
     * Store wrapper that logs get/update/increment/decrement operations at the time they actually hit storage.
     * Uses thread-local binding to attribute operations to the current transaction ID.
     */
    static class MockKVStore extends KVStore {
        private final TriConsumer<Integer, String, Character> onOp;  // (txid, key, opKind)
        private final Supplier<Integer> getTxid;
        private final ThreadLocal<Integer> tls = new ThreadLocal<>();

        @FunctionalInterface
        interface TriConsumer<A, B, C> {
            void accept(A a, B b, C c);
        }

        MockKVStore(TriConsumer<Integer, String, Character> onOp, Supplier<Integer> getTxid) {
            this.onOp = onOp;
            this.getTxid = getTxid;
        }

        void bindTxid(int txid) {
            tls.set(txid);
        }

        Integer currentTxid() {
            return tls.get();
        }

        private void logOp(String key, char kind) {
            Integer txid = currentTxid();
            if (txid == null) {
                txid = getTxid.get();
            }
            if (txid != null && txid >= 0) {
                onOp.accept(txid, key, kind);
            }
        }

        @Override
        public Object get(String key) {
            Object val = super.get(key);
            logOp(key, 'R');
            return val;
        }

        @Override
        public Object insert(String key, Object value) {
            logOp(key, 'W');
            return super.insert(key, value);
        }

        @Override
        public Object update(String key, Object value) {
            logOp(key, 'W');
            return super.update(key, value);
        }

        @Override
        public Long increment(String key, long delta) {
            logOp(key, 'I');
            return super.increment(key, delta);
        }

        @Override
        public Long decrement(String key, long delta) {
            logOp(key, 'D');
            return super.decrement(key, delta);
        }

        @Override
        public Object delete(String key) {
            logOp(key, 'W');
            return super.delete(key);
        }

        /**
         * Convenience method to update multiple key-value pairs.
         * Used by CC implementations to apply write sets at commit time.
         */
        public void updateAll(Map<String, Object> writes) {
            for (Map.Entry<String, Object> entry : writes.entrySet()) {
                update(entry.getKey(), entry.getValue());
            }
        }
    }

    // ---------- ConcurrentRunner ----------
    
    /**
     * Runs a concurrent workload against a single store and a CC instance,
     * logging read/write operations for building the precedence graph.
     * Only committed transactions' operations are considered for serializability.
     */
    static class ConcurrentRunner {
        private final Supplier<ConcurrencyControl> ccFactory;
        private final int numWorkers;
        private final double durationS;
        private final double stallThresholdS;

        // Thread-local operation buffers to avoid contention
        private final AtomicInteger globalSeq = new AtomicInteger(0);
        private final ConcurrentHashMap<Long, List<Op>> threadOps = new ConcurrentHashMap<>();
        private final ThreadLocal<List<Op>> tlsOps = ThreadLocal.withInitial(ArrayList::new);

        private final ThreadLocal<Integer> tlsTxnTxid = new ThreadLocal<>();
        final MockKVStore store;
        final ConcurrencyControl cc;

        private final Set<Integer> committed = ConcurrentHashMap.newKeySet();
        private final Set<Integer> aborted = ConcurrentHashMap.newKeySet();

        // Progress tracking
        private final ThreadLocal<Integer> progressSlotTls = new ThreadLocal<>();
        private final AtomicLongArray progressSlots;
        private final ConcurrentHashMap<Long, Integer> slotForTid = new ConcurrentHashMap<>();
        private final AtomicInteger nextSlot = new AtomicInteger(0);

        // Error propagation
        private volatile Throwable error = null;
        private volatile String errorTb = null;

        // Timing metrics
        private final AtomicLong totalReadTimeNanos = new AtomicLong(0);
        private final AtomicLong readCount = new AtomicLong(0);
        private final AtomicLong totalWriteTimeNanos = new AtomicLong(0);
        private final AtomicLong writeCount = new AtomicLong(0);
        private final AtomicLong totalCommitTimeNanos = new AtomicLong(0);
        private final AtomicLong commitCount = new AtomicLong(0);
        private final AtomicLong totalRollbackTimeNanos = new AtomicLong(0);
        private final AtomicLong rollbackCount = new AtomicLong(0);
        private final AtomicLong totalTransactionTimeNanos = new AtomicLong(0);
        private final AtomicLong transactionCount = new AtomicLong(0);
        private final ThreadLocal<Long> txnStartTimeNanos = new ThreadLocal<>();

        ConcurrentRunner(Supplier<ConcurrencyControl> ccFactory, int numWorkers, double durationS, double stallThresholdS) {
            this.ccFactory = ccFactory;
            this.numWorkers = numWorkers;
            this.durationS = durationS;
            this.stallThresholdS = stallThresholdS;
            this.progressSlots = new AtomicLongArray(numWorkers);

            MockKVStore.TriConsumer<Integer, String, Character> onOp = (txid, key, kind) -> {
                int seqNum = globalSeq.incrementAndGet();
                tlsOps.get().add(new Op(seqNum, txid, kind, key));
            };

            Supplier<Integer> getTxid = () -> tlsTxnTxid.get();

            this.store = new MockKVStore(onOp, getTxid);
            this.cc = ccFactory.get();
        }

        private void registerThreadOps() {
            List<Op> localOps = tlsOps.get();
            if (!localOps.isEmpty()) {
                threadOps.put(Thread.currentThread().getId(), new ArrayList<>(localOps));
            }
        }

        void initStore(Consumer<KVStore> initializer) {
            if (initializer != null) {
                initializer.accept(store);
            }
        }

        RunResult run(BiConsumer<ConcurrentRunner, Transaction> workload) {
            AtomicBoolean stop = new AtomicBoolean(false);
            List<Thread> threads = new ArrayList<>();
            long startTime = System.nanoTime();

            for (int i = 0; i < numWorkers; i++) {
                Thread t = new Thread(() -> {
                    long tid = Thread.currentThread().getId();
                    // Assign a slot for this thread
                    int slot = nextSlot.getAndIncrement();
                    slotForTid.put(tid, slot);
                    progressSlotTls.set(slot);
                    progressSlots.set(slot, System.nanoTime());

                    try {
                        while (!stop.get()) {
                            try {
                                txnStartTimeNanos.set(System.nanoTime());
                                Transaction txn = cc.begin(store);
                                tlsTxnTxid.set(txn.txid);
                                store.bindTxid(txn.txid);
                                workload.accept(this, txn);
                            } catch (Exception e) {
                                synchronized (this) {
                                    if (error == null) {
                                        error = e;
                                        errorTb = getStackTrace(e);
                                        stop.set(true);
                                    }
                                }
                                break;
                            } finally {
                                tlsTxnTxid.set(null);
                                store.bindTxid(-1);
                            }
                        }
                    } finally {
                        registerThreadOps();
                    }
                });
                t.setDaemon(true);
                threads.add(t);
                t.start();
            }

            // Wait for duration or early-exit on first error
            long deadlineNanos = startTime + (long) (durationS * 1_000_000_000L);
            while (System.nanoTime() < deadlineNanos && !stop.get()) {
                if (error != null) break;
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            stop.set(true);

            for (Thread t : threads) {
                try {
                    t.join(1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

            if (error != null) {
                throw new RuntimeException("ConcurrentRunner failed with error: " + error + "\n" + errorTb, error);
            }
            System.out.println("ConcurrentRunner finished");

            // Detect stalled threads
            System.out.println("Detecting stalled threads");
            long now = System.nanoTime();
            Set<Long> stalled = new HashSet<>();
            for (Thread t : threads) {
                Long tid = t.getId();
                Integer slot = slotForTid.get(tid);
                if (slot == null) continue;
                long last = progressSlots.get(slot);
                if ((now - last) / 1_000_000_000.0 > stallThresholdS) {
                    stalled.add(tid);
                }
            }

            // Parallel merge all thread-local ops and sort by sequence number using 64 threads
            ForkJoinPool sortPool = new ForkJoinPool(64);
            List<Op> committedOps;
            try {
                committedOps = sortPool.submit(() -> {
                    // Parallel merge: collect all ops from all threads
                    Op[] allOpsArray = threadOps.values().parallelStream()
                            .flatMap(List::stream)
                            .toArray(Op[]::new);
                    
                    // Parallel sort by sequence number using Arrays.parallelSort
                    Arrays.parallelSort(allOpsArray, Comparator.comparingInt(op -> op.seq));
                    
                    // Parallel filter to only committed transactions
                    return Arrays.stream(allOpsArray).parallel()
                            .filter(op -> committed.contains(op.txid))
                            .collect(Collectors.toList());
                }).get();
                System.out.println("Committed ops: " + committedOps.size());
            } catch (Exception e) {
                throw new RuntimeException("Failed to process ops", e);
            } finally {
                sortPool.shutdown();
            }




            return new RunResult(committedOps, new HashSet<>(committed), stalled,
                    getAvgReadTimeNanos(), getAvgWriteTimeNanos(),
                    getAvgCommitTimeNanos(), getAvgRollbackTimeNanos(),
                    getAvgTransactionTimeNanos());
        }

        private void touchProgress() {
            Integer slot = progressSlotTls.get();
            if (slot != null && slot >= 0 && slot < progressSlots.length()) {
                progressSlots.set(slot, System.nanoTime());
            }
        }

        Object logRead(Transaction txn, String key) {
            long start = System.nanoTime();
            Object val = cc.read(txn, key, store);
            touchProgress();
            long elapsed = System.nanoTime() - start;
            totalReadTimeNanos.addAndGet(elapsed);
            readCount.incrementAndGet();
            return val;
        }

        void logWrite(Transaction txn, String key, Object value) {
            long start = System.nanoTime();
            cc.write(txn, key, value, store);
            touchProgress();
            long elapsed = System.nanoTime() - start;
            totalWriteTimeNanos.addAndGet(elapsed);
            writeCount.incrementAndGet();
        }

        CommitResult logCommit(Transaction txn) {
            long start = System.nanoTime();
            tlsTxnTxid.set(txn.txid);
            store.bindTxid(txn.txid);
            CommitResult res = cc.commit(txn, store);
            if (res.ok) {
                committed.add(txn.txid);
            } else {
                aborted.add(txn.txid);
            }
            touchProgress();
            long elapsed = System.nanoTime() - start;
            totalCommitTimeNanos.addAndGet(elapsed);
            commitCount.incrementAndGet();
            // Record transaction time
            Long txnStart = txnStartTimeNanos.get();
            if (txnStart != null) {
                long txnElapsed = System.nanoTime() - txnStart;
                totalTransactionTimeNanos.addAndGet(txnElapsed);
                transactionCount.incrementAndGet();
            }
            return res;
        }

        void logRollback(Transaction txn) {
            long start = System.nanoTime();
            cc.rollback(txn, store);
            aborted.add(txn.txid);
            touchProgress();
            long elapsed = System.nanoTime() - start;
            totalRollbackTimeNanos.addAndGet(elapsed);
            rollbackCount.incrementAndGet();
            // Record transaction time
            Long txnStart = txnStartTimeNanos.get();
            if (txnStart != null) {
                long txnElapsed = System.nanoTime() - txnStart;
                totalTransactionTimeNanos.addAndGet(txnElapsed);
                transactionCount.incrementAndGet();
            }
        }

        double getAvgReadTimeNanos() {
            long count = readCount.get();
            return count > 0 ? (double) totalReadTimeNanos.get() / count : 0.0;
        }

        double getAvgWriteTimeNanos() {
            long count = writeCount.get();
            return count > 0 ? (double) totalWriteTimeNanos.get() / count : 0.0;
        }

        double getAvgCommitTimeNanos() {
            long count = commitCount.get();
            return count > 0 ? (double) totalCommitTimeNanos.get() / count : 0.0;
        }

        double getAvgRollbackTimeNanos() {
            long count = rollbackCount.get();
            return count > 0 ? (double) totalRollbackTimeNanos.get() / count : 0.0;
        }

        double getAvgTransactionTimeNanos() {
            long count = transactionCount.get();
            return count > 0 ? (double) totalTransactionTimeNanos.get() / count : 0.0;
        }

        private static String getStackTrace(Throwable t) {
            StringBuilder sb = new StringBuilder();
            for (StackTraceElement e : t.getStackTrace()) {
                sb.append(e.toString()).append("\n");
            }
            return sb.toString();
        }
    }

    static class RunResult {
        final List<Op> ops;
        final Set<Integer> committed;
        final Set<Long> stalled;
        final double avgReadTimeNanos;
        final double avgWriteTimeNanos;
        final double avgCommitTimeNanos;
        final double avgRollbackTimeNanos;
        final double avgTransactionTimeNanos;

        RunResult(List<Op> ops, Set<Integer> committed, Set<Long> stalled,
                  double avgReadTimeNanos, double avgWriteTimeNanos,
                  double avgCommitTimeNanos, double avgRollbackTimeNanos,
                  double avgTransactionTimeNanos) {
            this.ops = ops;
            this.committed = committed;
            this.stalled = stalled;
            this.avgReadTimeNanos = avgReadTimeNanos;
            this.avgWriteTimeNanos = avgWriteTimeNanos;
            this.avgCommitTimeNanos = avgCommitTimeNanos;
            this.avgRollbackTimeNanos = avgRollbackTimeNanos;
            this.avgTransactionTimeNanos = avgTransactionTimeNanos;
        }
    }

    // ---------- Precedence graph ----------

    static class KeyState {
        Integer lastWriter = null;
        Integer lastWriterSeq = null;
        Map<Integer, Integer> readers = new HashMap<>(); // txid -> first read seq in this epoch
    }

    static class Evidence {
        final String key;
        final int cause;  // 0=W→R, 1=W→W, 2=R→W
        final int fromSeq;
        final int toSeq;

        Evidence(String key, int cause, int fromSeq, int toSeq) {
            this.key = key;
            this.cause = cause;
            this.fromSeq = fromSeq;
            this.toSeq = toSeq;
        }
    }

    static class GraphResult {
        final Map<Integer, Set<Integer>> graph;
        final Map<Long, List<Evidence>> evidence;  // edge key = (u << 32) | v

        GraphResult(Map<Integer, Set<Integer>> graph, Map<Long, List<Evidence>> evidence) {
            this.graph = graph;
            this.evidence = evidence;
        }

        static long edgeKey(int u, int v) {
            return ((long) u << 32) | (v & 0xFFFFFFFFL);
        }
    }

    /**
     * Parallel version - processes each key independently in parallel.
     * Assumes ops are only from committed txns and in increasing seq order.
     * 
     * Conflict/Commutativity matrix for same item X, different transactions:
     * 
     *   op1 \ op2  |  R(X)  |  W(X)  |  Δ(X)=INC/DEC
     *   ---------- | ------ | ------ | -------------
     *   R(X)       |   no   |  yes   |     yes
     *   W(X)       |  yes   |  yes   |     yes
     *   Δ(X)       |  yes   |  yes   |  no (commutative)
     * 
     * State per item:
     * - lastSetW: last normal write (set/assign) txid and seq
     * - deltaWriters: set of txns that did INC/DEC since lastSetW
     * - readers: set of txns that read since last reset
     */
    static GraphResult buildPrecedenceGraphFast(List<Op> ops) {
        // Collect all txids for graph nodes
        Set<Integer> allTxids = ConcurrentHashMap.newKeySet();
        
        // Group operations by key in parallel
        Map<String, List<Op>> opsByKey = ops.parallelStream()
                .peek(op -> allTxids.add(op.txid))
                .collect(Collectors.groupingByConcurrent(op -> op.key));
        
        // Sort each key's ops by seq (needed for correct ordering within key)
        opsByKey.values().parallelStream().forEach(list -> 
                list.sort(Comparator.comparingInt(op -> op.seq)));
        
        // Thread-safe result containers
        ConcurrentHashMap<Integer, Set<Integer>> graph = new ConcurrentHashMap<>();
        ConcurrentHashMap<Long, List<Evidence>> evidence = new ConcurrentHashMap<>();
        
        // Initialize graph nodes
        allTxids.parallelStream().forEach(txid -> 
                graph.computeIfAbsent(txid, k -> ConcurrentHashMap.newKeySet()));
        
        // Process each key's operations in parallel
        ForkJoinPool pool = new ForkJoinPool(80);
        try {
            pool.submit(() -> opsByKey.entrySet().parallelStream().forEach(entry -> {
                String key = entry.getKey();
                List<Op> keyOps = entry.getValue();
                
                // State for this key
                Integer lastSetW = null;           // txid of last normal write (W)
                Integer lastSetWSeq = null;        // seq of last normal write
                Map<Integer, Integer> deltaWriters = new HashMap<>();  // txid -> first delta seq since lastSetW
                Map<Integer, Integer> readers = new HashMap<>();       // txid -> first read seq since last reset
                
                for (Op op : keyOps) {
                    if (op.kind == 'R') {
                        // R_i(X): Read operation
                        
                        // Edge from lastSetW → i (normal write before read)
                        if (lastSetW != null && lastSetW != op.txid) {
                            graph.computeIfAbsent(lastSetW, k -> ConcurrentHashMap.newKeySet()).add(op.txid);
                            long ek = GraphResult.edgeKey(lastSetW, op.txid);
                            evidence.computeIfAbsent(ek, k -> Collections.synchronizedList(new ArrayList<>()))
                                    .add(new Evidence(key, 0, lastSetWSeq != null ? lastSetWSeq : -1, op.seq));
                        }
                        
                        // Edges from all deltaWriters → i (deltas before read)
                        for (Map.Entry<Integer, Integer> deltaEntry : deltaWriters.entrySet()) {
                            int k = deltaEntry.getKey();
                            int kseq = deltaEntry.getValue();
                            if (k != op.txid) {
                                graph.computeIfAbsent(k, x -> ConcurrentHashMap.newKeySet()).add(op.txid);
                                long ek = GraphResult.edgeKey(k, op.txid);
                                evidence.computeIfAbsent(ek, x -> Collections.synchronizedList(new ArrayList<>()))
                                        .add(new Evidence(key, 3, kseq, op.seq));  // 3 = Δ→R
                            }
                        }
                        
                        readers.putIfAbsent(op.txid, op.seq);
                        
                    } else if (op.kind == 'I' || op.kind == 'D') {
                        // Δ_i(X): Delta operation (INC/DEC)
                        
                        // Edge from lastSetW → i (normal write before delta)
                        if (lastSetW != null && lastSetW != op.txid) {
                            graph.computeIfAbsent(lastSetW, k -> ConcurrentHashMap.newKeySet()).add(op.txid);
                            long ek = GraphResult.edgeKey(lastSetW, op.txid);
                            evidence.computeIfAbsent(ek, k -> Collections.synchronizedList(new ArrayList<>()))
                                    .add(new Evidence(key, 1, lastSetWSeq != null ? lastSetWSeq : -1, op.seq));
                        }
                        
                        // Edges from all readers → i (read before delta)
                        for (Map.Entry<Integer, Integer> readerEntry : readers.entrySet()) {
                            int r = readerEntry.getKey();
                            int rseq = readerEntry.getValue();
                            if (r != op.txid) {
                                graph.computeIfAbsent(r, k -> ConcurrentHashMap.newKeySet()).add(op.txid);
                                long ek = GraphResult.edgeKey(r, op.txid);
                                evidence.computeIfAbsent(ek, k -> Collections.synchronizedList(new ArrayList<>()))
                                        .add(new Evidence(key, 2, rseq, op.seq));  // 2 = R→Δ
                            }
                        }
                        
                        // Do NOT add edges from deltaWriters (Δ–Δ is commutative!)
                        
                        deltaWriters.putIfAbsent(op.txid, op.seq);
                        
                    } else {  // 'W' - normal write (assignment/overwrite)
                        // W_i(X): Normal write operation
                        
                        // Edge from lastSetW → i (WW conflict)
                        if (lastSetW != null && lastSetW != op.txid) {
                            graph.computeIfAbsent(lastSetW, k -> ConcurrentHashMap.newKeySet()).add(op.txid);
                            long ek = GraphResult.edgeKey(lastSetW, op.txid);
                            evidence.computeIfAbsent(ek, k -> Collections.synchronizedList(new ArrayList<>()))
                                    .add(new Evidence(key, 1, lastSetWSeq != null ? lastSetWSeq : -1, op.seq));
                        }
                        
                        // Edges from all readers → i (RW conflict)
                        for (Map.Entry<Integer, Integer> readerEntry : readers.entrySet()) {
                            int r = readerEntry.getKey();
                            int rseq = readerEntry.getValue();
                            if (r != op.txid) {
                                graph.computeIfAbsent(r, k -> ConcurrentHashMap.newKeySet()).add(op.txid);
                                long ek = GraphResult.edgeKey(r, op.txid);
                                evidence.computeIfAbsent(ek, k -> Collections.synchronizedList(new ArrayList<>()))
                                        .add(new Evidence(key, 2, rseq, op.seq));
                            }
                        }
                        
                        // Edges from all deltaWriters → i (Δ before overwrite)
                        for (Map.Entry<Integer, Integer> deltaEntry : deltaWriters.entrySet()) {
                            int k = deltaEntry.getKey();
                            int kseq = deltaEntry.getValue();
                            if (k != op.txid) {
                                graph.computeIfAbsent(k, x -> ConcurrentHashMap.newKeySet()).add(op.txid);
                                long ek = GraphResult.edgeKey(k, op.txid);
                                evidence.computeIfAbsent(ek, x -> Collections.synchronizedList(new ArrayList<>()))
                                        .add(new Evidence(key, 4, kseq, op.seq));  // 4 = Δ→W
                            }
                        }
                        
                        // Reset state for this key
                        readers.clear();
                        deltaWriters.clear();
                        lastSetW = op.txid;
                        lastSetWSeq = op.seq;
                    }
                }
            })).get();
        } catch (Exception e) {
            throw new RuntimeException("Failed to build precedence graph", e);
        } finally {
            pool.shutdown();
        }
        
        // Convert to regular HashMap with HashSet for return
        Map<Integer, Set<Integer>> resultGraph = new HashMap<>();
        for (Map.Entry<Integer, Set<Integer>> entry : graph.entrySet()) {
            resultGraph.put(entry.getKey(), new HashSet<>(entry.getValue()));
        }
        
        Map<Long, List<Evidence>> resultEvidence = new HashMap<>(evidence);
        
        return new GraphResult(resultGraph, resultEvidence);
    }

    static boolean hasCycleKahn(Map<Integer, Set<Integer>> graph) {
        Map<Integer, Integer> indeg = new HashMap<>();
        for (int u : graph.keySet()) {
            indeg.putIfAbsent(u, 0);
        }
        for (Map.Entry<Integer, Set<Integer>> entry : graph.entrySet()) {
            for (int v : entry.getValue()) {
                indeg.merge(v, 1, Integer::sum);
                graph.computeIfAbsent(v, k -> new HashSet<>());
            }
        }

        Deque<Integer> q = new ArrayDeque<>();
        for (Map.Entry<Integer, Integer> entry : indeg.entrySet()) {
            if (entry.getValue() == 0) {
                q.add(entry.getKey());
            }
        }

        int seen = 0;
        while (!q.isEmpty()) {
            int u = q.poll();
            seen++;
            Set<Integer> neighbors = graph.get(u);
            if (neighbors != null) {
                for (int v : neighbors) {
                    int newDeg = indeg.get(v) - 1;
                    indeg.put(v, newDeg);
                    if (newDeg == 0) {
                        q.add(v);
                    }
                }
            }
        }
        return seen != indeg.size();
    }

    static String explainCycleKahn(Map<Integer, Set<Integer>> graph, Map<Long, List<Evidence>> evidence, List<Op> ops) {
        // Kahn to find remaining subgraph nodes
        Map<Integer, Integer> indeg = new HashMap<>();
        for (int u : graph.keySet()) {
            indeg.putIfAbsent(u, 0);
        }
        for (Map.Entry<Integer, Set<Integer>> entry : graph.entrySet()) {
            for (int v : entry.getValue()) {
                indeg.merge(v, 1, Integer::sum);
                graph.computeIfAbsent(v, k -> new HashSet<>());
            }
        }

        Deque<Integer> q = new ArrayDeque<>();
        for (Map.Entry<Integer, Integer> entry : indeg.entrySet()) {
            if (entry.getValue() == 0) {
                q.add(entry.getKey());
            }
        }
        while (!q.isEmpty()) {
            int u = q.poll();
            Set<Integer> neighbors = graph.get(u);
            if (neighbors != null) {
                for (int v : neighbors) {
                    int newDeg = indeg.get(v) - 1;
                    indeg.put(v, newDeg);
                    if (newDeg == 0) {
                        q.add(v);
                    }
                }
            }
        }

        Set<Integer> remaining = new HashSet<>();
        for (Map.Entry<Integer, Integer> entry : indeg.entrySet()) {
            if (entry.getValue() > 0) {
                remaining.add(entry.getKey());
            }
        }
        if (remaining.isEmpty()) {
            return "Cycle present but could not isolate remaining subgraph.";
        }

        // Build subgraph over remaining nodes
        Map<Integer, List<Integer>> subAdj = new HashMap<>();
        for (int u : remaining) {
            List<Integer> vs = new ArrayList<>();
            Set<Integer> neighbors = graph.get(u);
            if (neighbors != null) {
                for (int v : neighbors) {
                    if (remaining.contains(v)) {
                        vs.add(v);
                    }
                }
            }
            subAdj.put(u, vs);
        }

        // Pick an edge to start
        Integer start = null;
        for (Map.Entry<Integer, List<Integer>> entry : subAdj.entrySet()) {
            if (!entry.getValue().isEmpty()) {
                start = entry.getKey();
                break;
            }
        }
        if (start == null) {
            return "Cycle present but no edges in residual graph.";
        }

        // Walk to find a cycle
        List<Integer> path = new ArrayList<>();
        Map<Integer, Integer> inPath = new HashMap<>();
        int cur = start;
        List<Integer> cycleNodes = null;

        while (true) {
            path.add(cur);
            inPath.put(cur, path.size() - 1);
            List<Integer> neighbors = subAdj.get(cur);
            if (neighbors == null || neighbors.isEmpty()) {
                Integer nextStart = null;
                for (Map.Entry<Integer, List<Integer>> entry : subAdj.entrySet()) {
                    if (!entry.getValue().isEmpty() && !inPath.containsKey(entry.getKey())) {
                        nextStart = entry.getKey();
                        break;
                    }
                }
                if (nextStart == null) {
                    return "Could not extract specific cycle path.";
                }
                cur = nextStart;
                continue;
            }
            int nxt = neighbors.get(0);
            if (inPath.containsKey(nxt)) {
                cycleNodes = new ArrayList<>(path.subList(inPath.get(nxt), path.size()));
                cycleNodes.add(nxt);
                break;
            }
            cur = nxt;
        }

        // Format using evidence
        StringBuilder sb = new StringBuilder();
        sb.append("Cycle txns: ").append(cycleNodes).append("\n");
        if (cycleNodes.size() > 10) {
            return sb.toString();
        }

        sb.append("Edges (with example evidence):\n");
        for (int i = 0; i < cycleNodes.size() - 1; i++) {
            int u = cycleNodes.get(i);
            int v = cycleNodes.get(i + 1);
            long ek = GraphResult.edgeKey(u, v);
            List<Evidence> evs = evidence.get(ek);
            if (evs == null || evs.isEmpty()) {
                sb.append("  ").append(u).append(" -> ").append(v).append(" (no evidence captured)\n");
                continue;
            }
            Evidence ev = evs.get(0);
            String causeName = causeName(ev.cause);
            sb.append("  ").append(u).append(" -> ").append(v)
              .append(" via ").append(causeName)
              .append(" on key '").append(ev.key).append("'")
              .append(" (from seq ").append(ev.fromSeq).append(", to seq ").append(ev.toSeq).append(")\n");
        }

        // Add per-transaction ops
        Set<Integer> txSet = new HashSet<>(cycleNodes);
        Map<Integer, List<Op>> txOps = new HashMap<>();
        for (Op op : ops) {
            if (txSet.contains(op.txid)) {
                txOps.computeIfAbsent(op.txid, k -> new ArrayList<>()).add(op);
            }
        }
        for (List<Op> opList : txOps.values()) {
            opList.sort(Comparator.comparingInt(o -> o.seq));
        }

        sb.append("Transactions ops:\n");
        for (int txid : cycleNodes) {
            List<Op> opList = txOps.getOrDefault(txid, Collections.emptyList());
            StringBuilder opsStr = new StringBuilder();
            for (Op o : opList) {
                if (opsStr.length() > 0) opsStr.append(" ");
                opsStr.append(o.kind).append("(").append(o.key).append(")");
            }
            sb.append("  T[").append(txid).append("]: ").append(opsStr).append("\n");
        }

        return sb.toString();
    }

    private static String causeName(int c) {
        switch (c) {
            case 0: return "W→R";
            case 1: return "W→W";
            case 2: return "R→W";
            case 3: return "Δ→R";
            case 4: return "Δ→W";
            default: return "?";
        }
    }

    // ---------- Workloads ----------

    static void wlCounter(ConcurrentRunner r, Transaction txn) {
        Object v = r.logRead(txn, "ctr");
        int val = (v instanceof Integer) ? (Integer) v : 0;
        // r.logWrite(txn, "ctr", val + 1);
        r.logCommit(txn);
    }

    static void wlBank(ConcurrentRunner r, Transaction txn) {
        Object vaObj = r.logRead(txn, "a");
        Object vbObj = r.logRead(txn, "b");
        int va = (vaObj instanceof Integer) ? (Integer) vaObj : 0;
        int vb = (vbObj instanceof Integer) ? (Integer) vbObj : 0;

        if (va == 0 && vb == 0) {
            r.logWrite(txn, "a", 500);
            r.logWrite(txn, "b", 500);
            r.logCommit(txn);
            return;
        }

        Random rand = ThreadLocalRandom.current();
        int x = rand.nextInt(5) + 1;
        if (rand.nextBoolean()) {
            va -= x;
            vb += x;
        } else {
            va += x;
            vb -= x;
        }
        r.logWrite(txn, "a", va);
        r.logWrite(txn, "b", vb);
        r.logCommit(txn);
    }

    static void wlDeadlockProne(ConcurrentRunner r, Transaction txn) {
        Random rand = ThreadLocalRandom.current();
        String first, second;
        if (rand.nextBoolean()) {
            first = "A";
            second = "B";
        } else {
            first = "B";
            second = "A";
        }

        Object v1Obj = r.logRead(txn, first);
        Object v2Obj = r.logRead(txn, second);
        int v1 = (v1Obj instanceof Integer) ? (Integer) v1Obj : 0;
        int v2 = (v2Obj instanceof Integer) ? (Integer) v2Obj : 0;

        r.logWrite(txn, first, v1 + 1);
        r.logWrite(txn, second, v2 + 1);
        r.logCommit(txn);
    }

    // ---------- TPC-C Workload ----------

    // Default TPC-C configuration
    static final int TPCC_NUM_WAREHOUSES = 8;
    static final int TPCC_DISTRICTS_PER_WH = 10;
    static final int TPCC_CUSTOMERS_PER_DIST = 100;
    static final int TPCC_NUM_ITEMS = 1000;
    static final int TPCC_INITIAL_STOCK = 100;
    static final int TPCC_INITIAL_BALANCE = 10000;
    static final int TPCC_INITIAL_YTD = 0;

    /**
     * TPC-C configuration class for parameterized benchmarks.
     * Allows customizing warehouse count and contention levels.
     */
    static class TpccConfig {
        final int numWarehouses;
        final int districtsPerWh;
        final int customersPerDist;
        final int numItems;
        final int hotItemCount;       // Number of "hot" items for contention (0 = uniform access)
        final double hotItemProb;     // Probability of accessing hot items (0.0-1.0)
        final int hotWarehouseCount;  // Number of "hot" warehouses (0 = uniform access)
        final double hotWarehouseProb; // Probability of accessing hot warehouses
        final String configName;      // Name for this configuration

        TpccConfig(int numWarehouses, int districtsPerWh, int customersPerDist, int numItems,
                   int hotItemCount, double hotItemProb, int hotWarehouseCount, double hotWarehouseProb,
                   String configName) {
            this.numWarehouses = numWarehouses;
            this.districtsPerWh = districtsPerWh;
            this.customersPerDist = customersPerDist;
            this.numItems = numItems;
            this.hotItemCount = hotItemCount;
            this.hotItemProb = hotItemProb;
            this.hotWarehouseCount = hotWarehouseCount;
            this.hotWarehouseProb = hotWarehouseProb;
            this.configName = configName;
        }

        // Factory methods for common configurations
        static TpccConfig withWarehouses(int numWarehouses) {
            return new TpccConfig(numWarehouses, 10, 100, 1000, 0, 0.0, 0, 0.0,
                    "tpcc_wh" + numWarehouses);
        }

        static TpccConfig withContention(String level) {
            switch (level) {
                case "very_low":
                    // Uniform access across all items and warehouses
                    return new TpccConfig(8, 10, 100, 1000, 0, 0.0, 0, 0.0,
                            "tpcc_contention_very_low");
                case "low":
                    // Slight hotspots: 10% of items get 30% of accesses
                    return new TpccConfig(8, 10, 100, 1000, 100, 0.3, 0, 0.0,
                            "tpcc_contention_low");
                case "medium":
                    // Moderate hotspots: 5% of items get 50% of accesses
                    return new TpccConfig(8, 10, 100, 1000, 50, 0.5, 2, 0.3,
                            "tpcc_contention_medium");
                case "high":
                    // High contention: 2% of items get 70% of accesses, 2 hot warehouses
                    return new TpccConfig(8, 10, 100, 1000, 20, 0.7, 2, 0.5,
                            "tpcc_contention_high");
                case "very_high":
                    // Very high contention: 1% of items get 90% of accesses, 1 hot warehouse
                    return new TpccConfig(8, 10, 100, 1000, 10, 0.9, 1, 0.7,
                            "tpcc_contention_very_high");
                case "extreme":
                    // Extreme contention: 5 items get 95% of accesses, 1 hot warehouse gets 90%
                    return new TpccConfig(8, 10, 100, 1000, 5, 0.95, 1, 0.9,
                            "tpcc_contention_extreme");
                default:
                    return new TpccConfig(8, 10, 100, 1000, 0, 0.0, 0, 0.0,
                            "tpcc_contention_default");
            }
        }

        /**
         * Select a warehouse ID based on contention configuration.
         */
        int selectWarehouse(Random rand) {
            if (hotWarehouseCount > 0 && rand.nextDouble() < hotWarehouseProb) {
                // Access a hot warehouse
                return rand.nextInt(hotWarehouseCount) + 1;
            }
            return rand.nextInt(numWarehouses) + 1;
        }

        /**
         * Select an item ID based on contention configuration.
         */
        int selectItem(Random rand) {
            if (hotItemCount > 0 && rand.nextDouble() < hotItemProb) {
                // Access a hot item
                return rand.nextInt(hotItemCount) + 1;
            }
            return rand.nextInt(numItems) + 1;
        }
    }

    // Thread-local TPC-C configuration for parameterized workloads
    static final ThreadLocal<TpccConfig> tlsTpccConfig = new ThreadLocal<>();

    static void initTpccData(KVStore store) {
        initTpccDataWithConfig(store, new TpccConfig(TPCC_NUM_WAREHOUSES, TPCC_DISTRICTS_PER_WH,
                TPCC_CUSTOMERS_PER_DIST, TPCC_NUM_ITEMS, 0, 0.0, 0, 0.0, "default"));
    }

    static void initTpccDataWithConfig(KVStore store, TpccConfig config) {
        Random rand = new Random(42);  // Fixed seed for reproducibility

        // Initialize warehouses
        for (int wid = 1; wid <= config.numWarehouses; wid++) {
            store.insert("w_" + wid + "_ytd", TPCC_INITIAL_YTD);

            // Initialize districts per warehouse
            for (int did = 1; did <= config.districtsPerWh; did++) {
                store.insert("d_" + wid + "_" + did + "_ytd", TPCC_INITIAL_YTD);
                store.insert("d_" + wid + "_" + did + "_next_oid", 1);

                // Initialize customers per district
                for (int cid = 1; cid <= config.customersPerDist; cid++) {
                    store.insert("c_" + wid + "_" + did + "_" + cid + "_bal", TPCC_INITIAL_BALANCE);
                    store.insert("c_" + wid + "_" + did + "_" + cid + "_ytd", TPCC_INITIAL_YTD);
                }
            }
        }

        // Initialize items (global, not per-warehouse)
        for (int iid = 1; iid <= config.numItems; iid++) {
            store.insert("item_" + iid + "_price", rand.nextInt(9901) + 100);  // 100-10000
        }

        // Initialize stock per warehouse per item
        for (int wid = 1; wid <= config.numWarehouses; wid++) {
            for (int iid = 1; iid <= config.numItems; iid++) {
                store.insert("s_" + wid + "_" + iid + "_qty", TPCC_INITIAL_STOCK);
            }
        }
    }

    static Consumer<KVStore> createTpccInitializer(TpccConfig config) {
        return store -> initTpccDataWithConfig(store, config);
    }

    static boolean tpccNewOrder(ConcurrentRunner r, Transaction txn) {
        TpccConfig config = tlsTpccConfig.get();
        if (config == null) {
            config = new TpccConfig(TPCC_NUM_WAREHOUSES, TPCC_DISTRICTS_PER_WH,
                    TPCC_CUSTOMERS_PER_DIST, TPCC_NUM_ITEMS, 0, 0.0, 0, 0.0, "default");
        }
        return tpccNewOrderWithConfig(r, txn, config);
    }

    static boolean tpccNewOrderWithConfig(ConcurrentRunner r, Transaction txn, TpccConfig config) {
        Random rand = ThreadLocalRandom.current();

        int wid = config.selectWarehouse(rand);
        int did = rand.nextInt(config.districtsPerWh) + 1;
        int cid = rand.nextInt(config.customersPerDist) + 1;

        // Read and update district's next order ID
        String nextOidKey = "d_" + wid + "_" + did + "_next_oid";
        Object nextOidObj = r.logRead(txn, nextOidKey);
        int nextOid = (nextOidObj instanceof Integer) ? (Integer) nextOidObj : 1;
        r.logWrite(txn, nextOidKey, nextOid + 1);
        int oid = nextOid;

        // Read customer balance
        String custBalKey = "c_" + wid + "_" + did + "_" + cid + "_bal";
        r.logRead(txn, custBalKey);

        // Generate order lines (5-15 items per order)
        int numItems = rand.nextInt(11) + 5;
        int totalAmount = 0;

        for (int olNum = 1; olNum <= numItems; olNum++) {
            int iid = config.selectItem(rand);

            // Read item price
            String itemPriceKey = "item_" + iid + "_price";
            Object priceObj = r.logRead(txn, itemPriceKey);
            int price = (priceObj instanceof Integer) ? (Integer) priceObj : 100;

            // Read and decrement stock
            String stockKey = "s_" + wid + "_" + iid + "_qty";
            Object stockQtyObj = r.logRead(txn, stockKey);
            int stockQty = (stockQtyObj instanceof Integer) ? (Integer) stockQtyObj : TPCC_INITIAL_STOCK;

            int olQty = rand.nextInt(10) + 1;

            int newQty = stockQty - olQty;
            if (newQty < 10) {
                newQty += 91;  // Restock
            }
            r.logWrite(txn, stockKey, newQty);

            totalAmount += price * olQty;
        }

        // Write order record
        String orderKey = "o_" + wid + "_" + did + "_" + oid + "_total";
        r.logWrite(txn, orderKey, totalAmount);

        return true;
    }

    static boolean tpccPayment(ConcurrentRunner r, Transaction txn) {
        TpccConfig config = tlsTpccConfig.get();
        if (config == null) {
            config = new TpccConfig(TPCC_NUM_WAREHOUSES, TPCC_DISTRICTS_PER_WH,
                    TPCC_CUSTOMERS_PER_DIST, TPCC_NUM_ITEMS, 0, 0.0, 0, 0.0, "default");
        }
        return tpccPaymentWithConfig(r, txn, config);
    }

    static boolean tpccPaymentWithConfig(ConcurrentRunner r, Transaction txn, TpccConfig config) {
        Random rand = ThreadLocalRandom.current();

        int wid = config.selectWarehouse(rand);
        int did = rand.nextInt(config.districtsPerWh) + 1;
        int cid = rand.nextInt(config.customersPerDist) + 1;

        int paymentAmount = rand.nextInt(499901) + 100;  // 100-500000

        // Update warehouse YTD
        String whYtdKey = "w_" + wid + "_ytd";
        Object whYtdObj = r.logRead(txn, whYtdKey);
        int whYtd = (whYtdObj instanceof Integer) ? (Integer) whYtdObj : 0;
        r.logWrite(txn, whYtdKey, whYtd + paymentAmount);

        // Update district YTD
        String distYtdKey = "d_" + wid + "_" + did + "_ytd";
        Object distYtdObj = r.logRead(txn, distYtdKey);
        int distYtd = (distYtdObj instanceof Integer) ? (Integer) distYtdObj : 0;
        r.logWrite(txn, distYtdKey, distYtd + paymentAmount);

        // Update customer balance and YTD payment
        String custBalKey = "c_" + wid + "_" + did + "_" + cid + "_bal";
        String custYtdKey = "c_" + wid + "_" + did + "_" + cid + "_ytd";

        Object custBalObj = r.logRead(txn, custBalKey);
        Object custYtdObj = r.logRead(txn, custYtdKey);
        int custBal = (custBalObj instanceof Integer) ? (Integer) custBalObj : TPCC_INITIAL_BALANCE;
        int custYtd = (custYtdObj instanceof Integer) ? (Integer) custYtdObj : 0;

        r.logWrite(txn, custBalKey, custBal - paymentAmount);
        r.logWrite(txn, custYtdKey, custYtd + paymentAmount);

        return true;
    }

    static void wlTpcc(ConcurrentRunner r, Transaction txn) {
        Random rand = ThreadLocalRandom.current();
        boolean success;

        if (rand.nextBoolean()) {
            success = tpccNewOrder(r, txn);
        } else {
            success = tpccPayment(r, txn);
        }

        if (success) {
            r.logCommit(txn);
        } else {
            r.logRollback(txn);
        }
    }

    /**
     * Creates a TPC-C workload with the specified configuration.
     * The config is set per-thread to allow concurrent workloads with different configs.
     */
    static BiConsumer<ConcurrentRunner, Transaction> createTpccWorkload(TpccConfig config) {
        return (r, txn) -> {
            tlsTpccConfig.set(config);
            try {
                Random rand = ThreadLocalRandom.current();
                boolean success;

                if (rand.nextBoolean()) {
                    success = tpccNewOrderWithConfig(r, txn, config);
                } else {
                    success = tpccPaymentWithConfig(r, txn, config);
                }

                if (success) {
                    r.logCommit(txn);
                } else {
                    r.logRollback(txn);
                }
            } finally {
                tlsTpccConfig.remove();
            }
        };
    }

    // ---------- Scenario running ----------

    static class ScenarioResult {
        final int committed;
        final int stalledThreads;
        final boolean hasCycle;
        final int ops;
        final double throughput;
        final double avgReadTimeNanos;
        final double avgWriteTimeNanos;
        final double avgCommitTimeNanos;
        final double avgRollbackTimeNanos;
        String cycleExplanation = null;

        ScenarioResult(int committed, int stalledThreads, boolean hasCycle, int ops, double throughput,
                       double avgReadTimeNanos, double avgWriteTimeNanos,
                       double avgCommitTimeNanos, double avgRollbackTimeNanos) {
            this.committed = committed;
            this.stalledThreads = stalledThreads;
            this.hasCycle = hasCycle;
            this.ops = ops;
            this.throughput = throughput;
            this.avgReadTimeNanos = avgReadTimeNanos;
            this.avgWriteTimeNanos = avgWriteTimeNanos;
            this.avgCommitTimeNanos = avgCommitTimeNanos;
            this.avgRollbackTimeNanos = avgRollbackTimeNanos;
        }

        Map<String, Object> toMap() {
            Map<String, Object> map = new LinkedHashMap<>();
            map.put("committed", committed);
            map.put("stalled_threads", stalledThreads);
            map.put("is_serializable", !hasCycle);
            map.put("ops", ops);
            map.put("throughput", throughput);
            map.put("avg_read_time_nanos", avgReadTimeNanos);
            map.put("avg_write_time_nanos", avgWriteTimeNanos);
            map.put("avg_commit_time_nanos", avgCommitTimeNanos);
            map.put("avg_rollback_time_nanos", avgRollbackTimeNanos);
            if (cycleExplanation != null) {
                map.put("cycle_explanation", cycleExplanation);
            }
            return map;
        }
    }

    static ScenarioResult runScenario(
            Supplier<ConcurrencyControl> ccFactory,
            BiConsumer<ConcurrentRunner, Transaction> workload,
            int numWorkers,
            double durationS,
            double stallThresholdS,
            Consumer<KVStore> storeInitializer) {

        ConcurrentRunner runner = new ConcurrentRunner(ccFactory, numWorkers, durationS, stallThresholdS);
        if (storeInitializer != null) {
            runner.initStore(storeInitializer);
        }

        System.out.println("Running workload");
        RunResult result = runner.run(workload);


        List<Op> opsToUse;
        if (result.ops.size() > 10000) {
            opsToUse = result.ops.subList(result.ops.size() - 10000, result.ops.size());
        } else {
            opsToUse = result.ops;
        }
        System.out.println("Ops to use: " + opsToUse.size());
        GraphResult graphResult = buildPrecedenceGraphFast(opsToUse);
        System.out.println("Precedence graph built");
        
        boolean hasCycle = hasCycleKahn(graphResult.graph);
        System.out.println("Has cycle: " + hasCycle);
        
        ScenarioResult scenario = new ScenarioResult(
                result.committed.size(),
                result.stalled.size(),
                hasCycle,
                result.ops.size(),
                durationS > 0 ? result.committed.size() / durationS : 0.0,
                result.avgReadTimeNanos,
                result.avgWriteTimeNanos,
                result.avgCommitTimeNanos,
                result.avgRollbackTimeNanos
        );

        if (hasCycle) {
            String cycleExplanation = explainCycleKahn(graphResult.graph, graphResult.evidence, result.ops);
            if (cycleExplanation.length() < 1000) {  
                scenario.cycleExplanation = cycleExplanation;
            }
        }

        return scenario;
    }

    // ---------- EvaluationResult ----------

    public static class EvaluationResult {
        public final Map<String, Object> metrics;
        public final Map<String, Object> artifacts;

        public EvaluationResult(Map<String, Object> metrics, Map<String, Object> artifacts) {
            this.metrics = metrics;
            this.artifacts = artifacts;
        }

        @Override
        public String toString() {
            return "EvaluationResult{metrics=" + metrics + ", artifacts=" + artifacts + "}";
        }
    }

    // ---------- Main evaluate method ----------

    public static EvaluationResult evaluate(Supplier<ConcurrencyControl> ccFactory) {
        return evaluate(ccFactory, false);
    }

    private static double getThroughputScore(double throughput) {
        double MAX_TPCC_THROUGHPUT = 600_000.0;
        // return Math.pow(Math.log(throughput + 1) / Math.log(MAX_TPCC_THROUGHPUT + 1), 10);
        return throughput / (MAX_TPCC_THROUGHPUT);
    }

    public static EvaluationResult evaluate(Supplier<ConcurrencyControl> ccFactory, boolean correctnessOnly) {
        try {
            Map<String, ScenarioResult> scenarios = new LinkedHashMap<>();

            // Basic workloads
            scenarios.put("counters", runScenario(ccFactory, Evaluator::wlCounter, 64, 5.0, 0.5, null));
            scenarios.put("bank", runScenario(ccFactory, Evaluator::wlBank, 64, 5.0, 0.5, null));
            scenarios.put("deadlock", runScenario(ccFactory, Evaluator::wlDeadlockProne, 64, 5.0, 0.2, null));

            // TPC-C with default 8 warehouses (for backwards compatibility)
            // scenarios.put("tpcc", runScenario(ccFactory, Evaluator::wlTpcc, 64, 5.0, 0.5, Evaluator::initTpccData));

            // TPC-C with different warehouse counts (scalability benchmark)
            // int[] warehouseCounts = {1, 2, 4, 8, 16};
            int[] warehouseCounts = {};
            for (int whCount : warehouseCounts) {
                TpccConfig config = TpccConfig.withWarehouses(whCount);
                String scenarioName = config.configName;
                System.out.println("Running TPC-C with " + whCount + " warehouse(s)...");
                scenarios.put(scenarioName, runScenario(
                        ccFactory,
                        createTpccWorkload(config),
                        64,
                        5.0,
                        0.5,
                        createTpccInitializer(config)
                ));
            }

            if (!correctnessOnly) {
            // TPC-C with different contention levels (8 warehouses, varying hotspot intensity)
                String[] contentionLevels = {"very_low", "medium", "very_high"};
                for (String level : contentionLevels) {
                    TpccConfig config = TpccConfig.withContention(level);
                    String scenarioName = config.configName;
                    System.out.println("Running TPC-C with contention level: " + level + "...");
                    scenarios.put(scenarioName, runScenario(
                            ccFactory,
                            createTpccWorkload(config),
                            64,
                            5.0,
                            0.5,
                            createTpccInitializer(config)
                    ));
                }
            }

            int totalScenarios = scenarios.size();
            int correct = 0;
            double totalThroughput = 0.0;

            int tpccCoefficient = 5;
            int otherCoefficient = 0;
            int weightedScenarios = 0;

            Map<String, Double> throughputs = new LinkedHashMap<>();

            for (Map.Entry<String, ScenarioResult> entry : scenarios.entrySet()) {
                String name = entry.getKey();
                ScenarioResult s = entry.getValue();

                boolean serializable = !s.hasCycle;
                boolean stallFree = s.stalledThreads == 0;
                boolean progressMade = s.committed > 100;
                boolean isOk = serializable && stallFree && progressMade;

                throughputs.put(name, s.throughput);

                if (isOk) {
                    correct++;
                }

                // Weight TPC-C scenarios higher
                if (name.startsWith("tpcc")) {
                    totalThroughput += s.throughput * tpccCoefficient;
                    weightedScenarios += tpccCoefficient;
                } else {
                    totalThroughput += s.throughput * otherCoefficient;
                    weightedScenarios += otherCoefficient;
                }
            }

            double correctness = totalScenarios > 0 ? (double) correct / totalScenarios : 0.0;

            Map<String, Object> metrics = new LinkedHashMap<>();
            metrics.put("correctness", correctness);

            Map<String, Object> artifacts = new LinkedHashMap<>();

            // Always include scenario summaries
            Map<String, Object> scenarioSummaries = new LinkedHashMap<>();
            for (Map.Entry<String, ScenarioResult> entry : scenarios.entrySet()) {
                scenarioSummaries.put(entry.getKey(), entry.getValue().toMap());
            }
            Map<String, Double> tpccThroughputs = throughputs.entrySet().stream().filter(entry -> entry.getKey().startsWith("tpcc_")).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            artifacts.put("tpcc_avg_throughputs",  tpccThroughputs.values().stream().mapToDouble(Double::doubleValue).average().orElse(0.0));
            artifacts.put("scenario_summaries", scenarioSummaries);

            // Only add other metrics if not in correctness-only mode
            if (!correctnessOnly) {
                double avgThroughput = weightedScenarios > 0 ? totalThroughput / weightedScenarios : 0.0;

                // Map throughput to a bounded score
                double throughputScore = getThroughputScore(avgThroughput);
                double combinedScore = 0.5 * correctness + 0.5 * throughputScore;

                metrics.put("throughput_score", throughputScore);
                metrics.put("combined_score", combinedScore);
                if (throughputs.containsKey("tpcc_contention_very_high")) 
                    metrics.put("pre_known_transactions_under_high_contention", getThroughputScore(throughputs.get("tpcc_contention_very_high")));
                if (throughputs.containsKey("tpcc_wh8")) 
                    metrics.put("pre_known_transactions_normal_scenario", getThroughputScore(throughputs.get("tpcc_wh8")));
                else if (throughputs.containsKey("tpcc_contention_very_low"))
                    metrics.put("pre_known_transactions_normal_scenario", getThroughputScore(throughputs.get("tpcc_contention_very_low")));

                

                // Add warehouse scalability summary
                // Map<String, Double> warehouseScalability = new LinkedHashMap<>();
                // for (int whCount : warehouseCounts) {
                //     String key = "tpcc_wh" + whCount;
                //     if (throughputs.containsKey(key)) {
                //         warehouseScalability.put(key, throughputs.get(key));
                //     }
                // }
                // metrics.put("warehouse_scalability", warehouseScalability);

                // Add contention impact summary
                // Map<String, Double> contentionImpact = new LinkedHashMap<>();
                // for (String level : contentionLevels) {
                //     String key = "tpcc_contention_" + level;
                //     if (throughputs.containsKey(key)) {
                //         contentionImpact.put(key, throughputs.get(key));
                //     }
                // }
                // metrics.put("contention_impact", contentionImpact);
            }

            return new EvaluationResult(metrics, artifacts);

        } catch (Exception e) {
            e.printStackTrace();
            Map<String, Object> metrics = new LinkedHashMap<>();
            metrics.put("correctness", 0.0);

            Map<String, Object> artifacts = new LinkedHashMap<>();
            artifacts.put("error_type", e.getClass().getName());
            artifacts.put("error_message", e.getMessage());

            StringBuilder sb = new StringBuilder();
            for (StackTraceElement elem : e.getStackTrace()) {
                sb.append(elem.toString()).append("\n");
            }
            artifacts.put("full_traceback", sb.toString());


            if (!correctnessOnly) {
                metrics.put("throughput", 0.0);
                metrics.put("combined_score", 0.0);
            }

            return new EvaluationResult(metrics, artifacts);
        }
    }

    // ---------- JSON Serialization ----------

    private static String toJson(Object obj) {
        return toJson(obj, 0);
    }

    private static String toJson(Object obj, int indent) {
        if (obj == null) {
            return "null";
        }
        if (obj instanceof String) {
            return "\"" + escapeJson((String) obj) + "\"";
        }
        if (obj instanceof Number) {
            return obj.toString();
        }
        if (obj instanceof Boolean) {
            return obj.toString();
        }
        if (obj instanceof Map) {
            Map<?, ?> map = (Map<?, ?>) obj;
            if (map.isEmpty()) {
                return "{}";
            }
            StringBuilder sb = new StringBuilder();
            sb.append("{\n");
            String indentStr = "  ".repeat(indent + 1);
            boolean first = true;
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                if (!first) {
                    sb.append(",\n");
                }
                first = false;
                sb.append(indentStr);
                sb.append("\"").append(escapeJson(entry.getKey().toString())).append("\": ");
                sb.append(toJson(entry.getValue(), indent + 1));
            }
            sb.append("\n").append("  ".repeat(indent)).append("}");
            return sb.toString();
        }
        if (obj instanceof Collection) {
            Collection<?> coll = (Collection<?>) obj;
            if (coll.isEmpty()) {
                return "[]";
            }
            StringBuilder sb = new StringBuilder();
            sb.append("[\n");
            String indentStr = "  ".repeat(indent + 1);
            boolean first = true;
            for (Object item : coll) {
                if (!first) {
                    sb.append(",\n");
                }
                first = false;
                sb.append(indentStr);
                sb.append(toJson(item, indent + 1));
            }
            sb.append("\n").append("  ".repeat(indent)).append("]");
            return sb.toString();
        }
        // Fallback for other objects
        return "\"" + escapeJson(obj.toString()) + "\"";
    }

    private static String escapeJson(String s) {
        if (s == null) return "";
        StringBuilder sb = new StringBuilder();
        for (char c : s.toCharArray()) {
            switch (c) {
                case '"': sb.append("\\\""); break;
                case '\\': sb.append("\\\\"); break;
                case '\b': sb.append("\\b"); break;
                case '\f': sb.append("\\f"); break;
                case '\n': sb.append("\\n"); break;
                case '\r': sb.append("\\r"); break;
                case '\t': sb.append("\\t"); break;
                default:
                    if (c < ' ') {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
            }
        }
        return sb.toString();
    }

    // ---------- Main method ----------

    public static void main(String[] args) {
        String outputFile = "output.json";
        boolean correctnessOnly = false;
        
        // Parse arguments
        for (int i = 0; i < args.length; i++) {
            if ("--output".equals(args[i]) && i + 1 < args.length) {
                outputFile = args[i + 1];
                i++;
            } else if ("--correctness-only".equals(args[i])) {
                correctnessOnly = true;
            }
        }

        System.out.println("Running evaluator with GeneratedCC...");
        if (correctnessOnly) {
            System.out.println("Correctness-only mode enabled: only correctness metric will be reported.");
        }

        EvaluationResult result = evaluate(GeneratedCC::buildCc, correctnessOnly);

        System.out.println("Evaluation complete.");
        System.out.println("Metrics: " + result.metrics);

        // Write JSON to file
        Map<String, Object> output = new LinkedHashMap<>();
        output.put("metrics", result.metrics);
        output.put("artifacts", result.artifacts);
        String jsonContent = toJson(output);

        try {
            Files.writeString(Path.of(outputFile), jsonContent);
            System.out.println("Results written to: " + outputFile);
        } catch (IOException e) {
            System.err.println("Failed to write output file: " + e.getMessage());
            // Fallback to stdout
            System.out.println(jsonContent);
        }
    }
}
