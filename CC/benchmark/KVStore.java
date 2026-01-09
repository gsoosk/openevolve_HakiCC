package benchmark;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Simple in-memory key-value store (thread-safe, supports atomic operations on numeric values):
 *   - get(key): O(1) lookup
 *   - insert(key, value): O(1) insert 
 *   - update(key, value): O(1) update
 *   - increment(key, delta): O(1) atomic increment (for Numeric values)
 *   - decrement(key, delta): O(1) atomic decrement (for Numeric values)
 *   - delete(key): O(1) removal
 * Concurrency:
 *   - ConcurrentHashMap for thread-safe operations
 */
class KVStore {
    private static final int CONCURRENCY_LEVEL = 96;
    private static final int INITIAL_CAPACITY = 256;
    private static final float LOAD_FACTOR = 0.75f;

    private final ConcurrentHashMap<String, Object> data = new ConcurrentHashMap<>(INITIAL_CAPACITY, LOAD_FACTOR, CONCURRENCY_LEVEL);

    /**
     * Retrieves the current value of a key.
     * 
     * @param key The key to retrieve
     * @return The value associated with the key, or null if not found
     */
    public Object get(String key) {
        return data.get(key);
    }

    /**
     * Inserts a new key-value pair into the store.
     * 
     * @param key The key to insert
     * @param value The value to associate with the key - Numeric values will be wrapped in an AtomicLong to be used for atomic operations.
     * @return The previous value associated with the key, or null if there was none
     */
    public Object insert(String key, Object value) {
        if (value instanceof Number) {
            AtomicLong dataValue = new AtomicLong(((Number) value).longValue());
            data.put(key, dataValue);
            return dataValue;
        }
        
        return data.putIfAbsent(key, value);  
    }

    /**
     * Updates an existing key-value pair in the store.
     * 
     * @param key The key to update
     * @param value The new value to associate with the key
     * @return The previous value associated with the key, or null if there was none
     */
    public Object update(String key, Object value) {
        if (value instanceof Number) {
            AtomicLong dataValue = new AtomicLong(((Number) value).longValue());
            data.put(key, dataValue);
            return dataValue;
        }
        
        return data.put(key, value);
    }

    /**
     * Increments the AtomicLong value associated with the key by the specified delta.
     * 
     * @param key The key whose value should be incremented
     * @param delta The value to add
     * @return The new value after incrementing, or null if key not found or not an AtomicLong
     */
    public Long increment(String key, long delta) {
        Object value = data.get(key);
        if (value instanceof AtomicLong) {
            return ((AtomicLong) value).addAndGet(delta);
        }
        return null;
    }

    /**
     * Decrements the AtomicLong value associated with the key by the specified delta.
     * 
     * @param key The key whose value should be decremented
     * @param delta The value to subtract
     * @return The new value after decrementing, or null if key not found or not an AtomicLong
     */
    public Long decrement(String key, long delta) {
        Object value = data.get(key);
        if (value instanceof AtomicLong) {
            return ((AtomicLong) value).addAndGet(-delta);
        }
        return null;
    }

    /**
     * Deletes a key-value pair from the store.
     * 
     * @param key The key to delete
     * @return The value that was associated with the key, or null if there was none
     */
    public Object delete(String key) {
        return data.remove(key);
    }

    /**
     * Updates multiple key-value pairs in the store.
     * Convenience method for batch updates.
     * 
     * @param writes A map of key-value pairs to be updated
     */
    public void updateAll(Map<String, Object> writes) {
        for (Map.Entry<String, Object> entry : writes.entrySet()) {
            update(entry.getKey(), entry.getValue());
        }
    }
}
