package ae.teletronics.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import net.jcip.annotations.ThreadSafe;

// TODO java8 import java.util.function.Function;
// TODO java8 import java.util.function.Predicate;
// TODO java8 import java.util.function.Supplier;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.cache.Cache;
import com.google.common.collect.Interner;
import com.google.common.collect.Interners;

/**
 * Key-value cache, where values can change. Several threads can collaborate in building the values. Therefore put operation has been replaced by
 * modify operation - knowing about how to create the initial value if the entry is not already present in the cache, and knowing about how to 
 * modify the value (whether or not it existed in cache already)
 * 
 * @param <K> Type of the cache-key
 * @param <V> Type of the cache-value
 */
@ThreadSafe
public class ChangingValueCache<K, V> {
	
	/**
	 * Builder for building {@link ChangingValueCache} instances 
	 *
	 * @param <K> Type of the cache-key of the built cache
	 * @param <V> Type of the cache-value of the built cache
	 */
	public static class Builder<K, V> {
		
		protected final ChangingValueCache<K, V> instance;
		
		protected Builder() {
			instance = createInstance();
		}
		
		protected ChangingValueCache<K, V> createInstance() {
			return new ChangingValueCache<K, V>();
		}
		
		/**
		 * Set the default new-creator. Used to generate the cache-value, during modify operation when
		 * * the cache-entry does not already exist
		 * * and, the modify was not called with an explicit new-creator (overriding this default)
		 * 
		 * @param newCreator Used to generate the new cache-value
		 * @return This builder
		 */
		public Builder<K, V> defaultNewCreator(Supplier<V> newCreator) {
			instance.defaultNewCreator = newCreator;
			return this;
		}
		
		/**
		 * Set the default modifier. Used to modify the cache-value, during modify operation when
		 * * the modify was not called with an explicit modifier (overriding this default)
		 * 
		 * @param modifier Used to modify the cache-value
		 * @return This builder
		 */
		public Builder<K, V> defaultModifier(Function<V, V> modifier) {
			instance.defaultModifier = modifier;
			return this;
		}
		
		/**
		 * The the cache to be used internally
		 * @param cache The Guava cache to be used internally
		 * @return This builder
		 */
		public Builder<K, V> cache(Cache<K, V> cache) {
			instance.cache = cache;
			return this;
		}
		
		/**
		 * Build the {@link ChangingValueCache} instance
		 * @return The built {@link ChangingValueCache} instance
		 */
		public ChangingValueCache<K, V> build() {
			if (instance.getAllCaches().size() < 1)
				throw new RuntimeException("No inner cache(s) set");
			
			return instance;
		}
		
	}
	
	protected Supplier<V> defaultNewCreator;
	protected Function<V, V> defaultModifier;
	protected Cache<K, V> cache;
	
	protected final Interner<Integer> newOrMovingCacheEntryInterner;
	
	protected ChangingValueCache() {
		newOrMovingCacheEntryInterner = Interners.newWeakInterner();
	}
	
	/**
	 * Get a builder for building a {@link ChangingValueCache} instance
	 * 
 	 * @param <K> Type of the cache-key of the built cache
	 * @param <V> Type of the cache-value of the built cache
	 * 
	 * @return The builder to be used
	 */
	public static <K, V> Builder<K, V> builder() {
	    return new Builder<K, V>();
	}
	
	/**
	 * Calling {@link #modify(Object, Supplier, Function, boolean)} (and returning value from) with
	 * * key (K) Provided key
	 * * newCreator (Supplier{@literal <V>}) null
	 * * modifier (Function{@literal <V, V>}) null
	 * * createIfNotExists (boolean) Provided createIfNotExist
	 * 
	 * @param key To be forwarded to modify call
	 * @param createIfNotExists To be forwarded to modify call
	 * @return  The value now on the cache-entry
	 */
	public final V modify(K key, boolean createIfNotExists) {
		return modify(key, null, null, createIfNotExists);
	}

	/**
	 * Calling {@link #modify(Object, Supplier, Function, boolean)} (and returning value from) with
	 * * key (K) Provided key
	 * * newCreator (Supplier{@literal <V>}) null
	 * * modifier (Function{@literal <V, V>}) Provided modifier
	 * * createIfNotExists (boolean) Provided createIfNotExist
	 * 
	 * @param key To be forwarded to modify call
	 * @param modifier To be forwarded to modify call
	 * @param createIfNotExists To be forwarded to modify call
	 * @return  The value now on the cache-entry
	 */
	public final V modify(K key, Function<V, V> modifier, boolean createIfNotExists) {
		return modify(key, null, modifier, createIfNotExists);
	}

	/**
	 * Calling {@link #modify(Object, Supplier, Function, boolean)} (and returning value from) with
	 * * key (K) Provided key
	 * * newCreator (Supplier{@literal <V>}) Provided new-creator
	 * * modifier (Function{@literal <V, V>}) null
	 * * createIfNotExists (boolean) Provided createIfNotExist
	 * 
	 * @param key To be forwarded to modify call
	 * @param newCreator To be forwarded to modify call
	 * @param createIfNotExists To be forwarded to modify call
	 * @return  The value now on the cache-entry
	 */
	public final V modify(K key, Supplier<V> newCreator, boolean createIfNotExists) {
		return modify(key, newCreator, null, createIfNotExists);
	}

	/**
	 * Calling {@link #modify(Object, Supplier, Function, boolean, boolean)} (and returning value from) with
	 * * key (K) Provided key
	 * * newCreator (Supplier{@literal <V>}) Provided new-creator
	 * * modifier (Function{@literal <V, V>}) Provided modifier
	 * * createIfNotExists (boolean) Provided createIfNotExist
	 * * supportRecursiveCalls (boolean) false
	 * 
	 * @param key To be forwarded to modify call
	 * @param newCreator To be forwarded to modify call
	 * @param modifier To be forwarded to modify call
	 * @param createIfNotExists To be forwarded to modify call
	 * @return  The value now on the cache-entry
	 */
	public final V modify(K key, Supplier<V> newCreator, Function<V, V> modifier, boolean createIfNotExists) {
		return modify(key, newCreator, modifier, createIfNotExists, false);
	}
	
	/**
	 * Modify cache-entry using provided new-creator and modifier
	 * @param key Key for cache-entry
	 * @param newCreator Used to generate the new cache-value, if it does not already exist (if null default new-creator will be used)
	 * @param modifier Used to modify the cache-value (if null default modifier will be used)
	 * @param createIfNotExists Create the entry if it does not already exist
	 * @param supportRecursiveCalls Support modifier that (directly or indirectly) calls modify on THE SAME cache-key (and the same thread). 
	 * Be careful setting this to true
	 * * Modifier that calls modify on cache-key that is NOT the same will not work correctly - it will when set to false (default)
	 * * Will add a small performance-hit
	 * @return The value now on the cache-entry
	 */
	public final V modify(K key, Supplier<V> newCreator, Function<V, V> modifier, boolean createIfNotExists, boolean supportRecursiveCalls) {
		synchronized(newOrMovingCacheEntryInterner.intern(key.hashCode())) {
			return modifyImpl(key, newCreator, modifier, createIfNotExists, supportRecursiveCalls);
		}
	}
	
	/**
	 * Calling {@link #modifyAll(Predicate, Predicate, Function)} with
	 * * keyPredicate (Predicate{@literal <K>}) Provided keyPredicate
	 * * valuePredicate (Predicate{@literal <V>}) Provided valuePredicate
	 * * modifier (Function{@literal <V, V>}) null
	 * * supportRecursiveCalls (boolean) false
	 * 
	 * @param keyPredicate To be forwarded to modifyAll call
	 * @param valuePredicate To be forwarded to modifyAll call
	 */
	public final void modifyAll(Predicate<K> keyPredicate, Predicate<V> valuePredicate) {
		modifyAll(keyPredicate, valuePredicate, null);
	}

	/**
	 * Calling {@link #modifyAll(Predicate, Predicate, Function, boolean)} with
	 * * keyPredicate (Predicate{@literal <K>}) Provided keyPredicate
	 * * valuePredicate (Predicate{@literal <V>}) Provided valuePredicate
	 * * modifier (Function{@literal <V, V>}) Provided modifier
	 * * supportRecursiveCalls (boolean) false
	 * 
	 * @param keyPredicate To be forwarded to modifyAll call
	 * @param valuePredicate To be forwarded to modifyAll call
	 * @param modifier To be forwarded to modifyAll call
	 */
	public final void modifyAll(Predicate<K> keyPredicate, Predicate<V> valuePredicate, Function<V, V> modifier) {
		modifyAll(keyPredicate, valuePredicate, modifier, false);
	}

	/**
	 * Sequentially calling {@link #modify(Object, Supplier, Function, boolean, boolean)} for a selected set of existing cache-entries. Called with
	 * * key (K) The cache-key of the cache-entry in the selected set
	 * * newCreator (Supplier{@literal <V>}) null
	 * * modifier (Function{@literal <V, V>}) Provided modifier
	 * * boolean createIfNotExists false
	 * * boolean supportRecursiveCalls Provided supportRecursiveCalls 
	 * 
	 * @param keyPredicate A cache-entry is (potentially - if valuePredicate also accepts) included in the selected set iff feeding keyPredicate with cache-key returns true
	 * @param valuePredicate A cache-entry is (potentially - if keyPredicate also accepts) included in the selected set iff feeding valuePredicate with cache-value returns true
	 * @param modifier Used for the modify calls
	 * @param supportRecursiveCalls Used for the modify calls
	 * 
	 * The existing cache-entry has to match both criteria to be included in the set
	 */
	public final void modifyAll(Predicate<K> keyPredicate, Predicate<V> valuePredicate, Function<V, V> modifier, boolean supportRecursiveCalls) {
		for (Cache<K, V> cache : getAllCaches()) {
			for (Map.Entry<K, V> entry : cache.asMap().entrySet()) {
				if ((keyPredicate == null || keyPredicate.apply(entry.getKey())) &&
					(valuePredicate == null || valuePredicate.apply(entry.getValue()))) {
					modify(entry.getKey(), null, modifier, false, supportRecursiveCalls);
				}
			}
		}
	}
	
	protected ThreadLocal<V> alreadyWorkingOn = new ThreadLocal<V>();
	protected V modifyImpl(K key, Supplier<V> newCreator, Function<V, V> modifier, boolean createIfNotExists, boolean supportRecursiveCalls) {
		V value = alreadyWorkingOn.get();
		if (value != null) {
			V newValue = ((modifier != null)?modifier:defaultModifier).apply(value);
			if (newValue != value) throw new RuntimeException("Modifier called modify with a modifier that replaced value object with another value object");
		} else {
			value = getIfPresent(key);
			
			boolean created = false;
			if (value == null) {
				if (createIfNotExists) {
					value = ((newCreator != null)?newCreator:defaultNewCreator).get();
					created = true;
				}
			}
	
			if (value != null) {
				if (supportRecursiveCalls) alreadyWorkingOn.set(value);
				try {
					V newValue = ((modifier != null)?modifier:defaultModifier).apply(value);
					
					if (newValue == null) {
						cache.invalidate(key);
					} else {
						if (created || newValue != value) {
							cache.put(key, newValue);
						}
					}
					value = newValue;
				} finally {
					if (supportRecursiveCalls) alreadyWorkingOn.remove();
				}
			}
		}
		
		return value;
	}
	
	protected Collection<Cache<K, V>> getAllCaches() {
		Collection<Cache<K, V>> allCaches = new ArrayList<Cache<K, V>>();
		if (cache != null) {
			allCaches.add(cache);
		}
		return allCaches;
	}
	
	/**
	 * Get a cache-value of a cache-entry with a provided key
	 * @param key The key of the cache-entry
	 * @return The cache-value (or null if not present in cache)
	 */
	public V getIfPresent(K key) {
		for (Cache<K, V> cache : getAllCaches()) {
			V value = cache.getIfPresent(key);
			if (value != null) return value;
		}
		return null;
	}

	/**
	 * @return Number of cache-entries in the cache
	 */
	public long size() {
		long size = 0;
		for (Cache<K, V> cache : getAllCaches()) {
			size += cache.size();
		}
		return size;
	}
	
	/**
	 * Calling {@link #getAddIfNotPresent(Object, Supplier)} (and returning value from) with
	 * * key (K) Provided key
	 * * newCreator (Supplier{@literal <V>}) null
	 * 
	 * @param key To be forwarded to getAddIfNotPresent call
	 * @return The cache-value
	 */
	public V getAddIfNotPresent(K key) {
		return getAddIfNotPresent(key, null);
	}
	
	private class NoModificationModifier implements Function<V, V> {

		@Override
		public V apply(V input) {
			return input;
		}
		
	}
	private NoModificationModifier noModificationModifier = new NoModificationModifier(); 
	/**
	 * Get a cache-value of a cache-entry with a provided key, adding the cache-entry if not already present
	 * @param key The key of the cache entry
	 * @param newCreator Used to generate the new cache-value, if it does not already exist (if null default new-creator will be used)
	 * @return The cache-value
	 */
	public V getAddIfNotPresent(K key, Supplier<V> newCreator) {
		return modify(key, newCreator, noModificationModifier, true);
	}

}
