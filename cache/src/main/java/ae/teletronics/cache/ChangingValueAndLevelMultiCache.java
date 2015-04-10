package ae.teletronics.cache;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import net.jcip.annotations.ThreadSafe;
import ae.teletronics.cache.examples.dbversioncache.StringStringOptimisticLockingDBWithKeyStartsWithCache;
import ae.teletronics.cache.examples.dbversioncache.StringStringOptimisticLockingDBWithKeyStartsWithCacheTest;


//TODO java8 import java.util.function.Function;
//TODO java8 import java.util.function.Supplier;
//TODO java8 import java.util.function.BiFunction;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.cache.Cache;

/**
 * Same as {@link ChangingValueCache}, but with prioritization of the cache-entries. Cache-entries are given a priority (level) and non-overlapping
 * intervals of levels define different prioritizations. When a cache-value is modified the level and hence the prioritization of the cache-entry can change 
 * 
 * For examples of usage see the code of
 * * {@link StringStringOptimisticLockingDBWithKeyStartsWithCache} (and the test of it {@link StringStringOptimisticLockingDBWithKeyStartsWithCacheTest})
 *
 * @param <K> Type of the cache-key
 * @param <V> Type of the cache-value
 */
@ThreadSafe
public class ChangingValueAndLevelMultiCache<K, V> extends ChangingValueCache<K, V> {
	
	// TODO java8 Remove
	public static interface BiFunction<T, U, R> {

		R apply(T t, U u);
		
	}

	/**
	 * Builder for building {@link ChangingValueAndLevelMultiCache} instances 
	 *
	 * @param <K> Type of the cache-key of the built cache
	 * @param <V> Type of the cache-value of the built cache
	 */
	public static class Builder<K, V> extends ChangingValueCache.Builder<K, V> {
		
		@Override
		protected ChangingValueAndLevelMultiCache<K, V> createInstance() {
			return new ChangingValueAndLevelMultiCache<K, V>();
		}
		
		private ChangingValueAndLevelMultiCache<K, V> getInstance() {
			return (ChangingValueAndLevelMultiCache<K, V>)instance;
		}
		
		/**
		 * See {@link ChangingValueCache.Builder#defaultNewCreator(Supplier)}
		 */
		public Builder<K, V> defaultNewCreator(Supplier<V> newCreator) {
			return (Builder<K, V>)super.defaultNewCreator(newCreator);
		}

		/**
		 * See {@link ChangingValueCache.Builder#defaultModifier(Function)}
		 */
		public Builder<K, V> defaultModifier(Function<V, V> modifier) {
			return (Builder<K, V>)super.defaultModifier(modifier);
		}

		/**
		 * See {@link ChangingValueCache.Builder#cache(Cache)}. This Guava cache is the internal cache used
		 * in case a cache-entry has a level that does not fit any of the explicitly defined level-intervals
		 */
		public Builder<K, V> cache(Cache<K, V> cache) {
			return (Builder<K, V>)super.cache(cache);
		}
		
		/**
		 * Set the calculator used to calculate the level of a particular cache-entry
		 * @param levelCalculator Given the cache-key and cache-level calculate the level of the cache-entry
		 * @return The calculated level
		 */
		public Builder<K,V> levelCalculator(BiFunction<K, V, Integer> levelCalculator) {
			getInstance().levelCalculator = levelCalculator;
			return this;
		}
		
		/**
		 * Add an additional internal cache to be used for cache-entries with level within a specific interval.
		 * When a cache-entry is modified so that its level changes to be within levelFrom (inclusive) and
		 * levelTo (inclusive), the cache-entry will be moved to the provided cache. If when level afterwards
		 * moves outside the interval, it will be removed from the provided cache and into another internal
		 * cache - either another cache added using this addCache method, or the default cache
		 * @param cache The Guava cache to be used internally
		 * @param levelFrom The lower boundary on cache-entry-level for this cache to be used
		 * @param levelTo The higher boundary on cache-entry-level for this cache to be used
		 * @param name A logical name for the cache
		 * @return
		 */
		public Builder<K,V> addCache(Cache<K,V> cache, int levelFrom, int levelTo, String name) {
			getInstance().caches.put(new Interval(levelFrom, levelTo), cache);
			getInstance().names.put(cache, name);
			return this;
		}
		
		/**
		 * Build the {@link ChangingValueAndLevelMultiCache} instance
		 * @return The built {@link ChangingValueAndLevelMultiCache} instance
		 */
		public ChangingValueAndLevelMultiCache<K,V> build() {
			if (getInstance().levelCalculator == null) 
				throw new RuntimeException("No levelCalculator set");

			return (ChangingValueAndLevelMultiCache<K, V>)super.build();
		}
		
	}
	
	protected static class Interval {
		
		private int from;
		private int to;

		public Interval(int from, int to) {
			this.from = from;
			this.to = to;
		}

		public int getFrom() {
			return from;
		}

		public int getTo() {
			return to;
		}
		
		public boolean belongsIn(int value) {
			return value >= from && value <= to;
		}
		
	}
	
	protected Map<Interval, Cache<K, V>> caches = new HashMap<Interval, Cache<K ,V>>();
	protected Map<Cache<K, V>, String> names = new HashMap<Cache<K ,V>, String>();
	protected BiFunction<K, V, Integer> levelCalculator;
	
	/**
	 * Get a builder for building a {@link ChangingValueAndLevelMultiCache} instance
	 * @return The builder to be used
	 */
	public static <K, V> Builder<K, V> builder() {
	    return new Builder<K, V>();
	}
	
	@Override
	protected V modifyImpl(K key, Supplier<V> newCreator, Function<V, V> modifier, boolean createIfNotExists, boolean supportRecursiveCalls) {
		V value = alreadyWorkingOn.get();
		if (value != null) {
			V newValue = ((modifier != null)?modifier:defaultModifier).apply(value);
			if (newValue != value) throw new RuntimeException("Modifier called modify with a modifier that replaced value object with another value object");
		} else {
			Pair<Cache<K, V>, V> cacheAndValue = getCacheAndValueIfPresent(key);
			
			Cache<K, V> oldCache = null;
			if (cacheAndValue == null) {
				if (createIfNotExists) value = ((newCreator != null)?newCreator:defaultNewCreator).get();
			} else {
				oldCache = cacheAndValue._1;
				value = cacheAndValue._2;
			}
	
			if (value != null) {
				if (supportRecursiveCalls) alreadyWorkingOn.set(value);
				try {
					V newValue = ((modifier != null)?modifier:defaultModifier).apply(value);
					if (newValue == null) {
						if (oldCache != null) oldCache.invalidate(key);
					} else {
						Cache<K, V> newCache = cacheForLevel(levelCalculator.apply(key, newValue));
						if (oldCache != newCache || newValue != value) {
							if (oldCache != null && oldCache != newCache) oldCache.invalidate(key);
							if (newCache != null) newCache.put(key, newValue);
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
	
	@Override
	protected Collection<Cache<K, V>> getAllCaches() {
		Collection<Cache<K, V>> allCaches = super.getAllCaches();
		for (Cache<K, V> cache : caches.values()) {
			allCaches.add(cache);
		}
		return allCaches;
	}

	/**
	 * Get a cache-value and the internal cache it currently lives in of a cache-entry with a provided key
	 * @param key The key of the cache-entry
	 * @return The cache-value and the internal cache (or null if not present in cache)
	 */
	public Pair<Cache<K, V>, V> getCacheAndValueIfPresent(K key) {
		for (Cache<K, V> cache : getAllCaches()) {
			V value = cache.getIfPresent(key);
			if (value != null) return createCacheAndValuePair(cache, value);
		}
		
		return null;
	}
	
	protected Pair<Cache<K, V>, V> createCacheAndValuePair(Cache<K, V> cache, V value) {
		return new Pair<Cache<K, V>, V>(cache, value);
	}
	
	protected Cache<K, V> cacheForLevel(int level) {
		for (Map.Entry<Interval, Cache<K,V>> entry : caches.entrySet()) {
			if (entry.getKey().belongsIn(level)) return entry.getValue();
		}
		return cache;
	}


}
