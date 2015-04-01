package ae.teletronics.cache;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

//TODO java8 import java.util.function.Function;
//TODO java8 import java.util.function.Supplier;
//TODO java8 import java.util.function.BiFunction;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.cache.Cache;

public class ChangingValueAndLevelMultiCache<K, V> extends ChangingValueCache<K, V> {
	
	// TODO java8 Remove
	public static interface BiFunction<T, U, R> {

		R apply(T t, U u);
		
	}

	public static class Builder<K, V> extends ChangingValueCache.Builder<K, V> {
		
		@Override
		protected ChangingValueAndLevelMultiCache<K, V> createInstance() {
			return new ChangingValueAndLevelMultiCache<K, V>();
		}
		
		private ChangingValueAndLevelMultiCache<K, V> getInstance() {
			return (ChangingValueAndLevelMultiCache<K, V>)instance;
		}
		
		public Builder<K, V> defaultNewCreator(Supplier<V> newCreator) {
			return (Builder<K, V>)super.defaultNewCreator(newCreator);
		}
		
		public Builder<K, V> defaultModifier(Function<V, V> modifier) {
			return (Builder<K, V>)super.defaultModifier(modifier);
		}
		
		public Builder<K, V> cache(Cache<K, V> cache) {
			return (Builder<K, V>)super.cache(cache);
		}
		
		public Builder<K,V> levelCalculator(BiFunction<K, V, Integer> levelCalculator) {
			getInstance().levelCalculator = levelCalculator;
			return this;
		}
		
		public Builder<K,V> addCache(Cache<K,V> cache, int levelFrom, int levelTo, String name) {
			getInstance().caches.put(new Interval(levelFrom, levelTo), cache);
			getInstance().names.put(cache, name);
			return this;
		}
		
		public ChangingValueAndLevelMultiCache<K,V> build() {
			if (getInstance().levelCalculator == null) 
				throw new RuntimeException("No levelCalculator set");

			return (ChangingValueAndLevelMultiCache<K, V>)super.build();
		}
		
	}
	
	public static class Interval {
		
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
	
	public static <K, V> Builder<K, V> builder() {
	    return new Builder<K, V>();
	}
	
	@Override
	protected V modifyImpl(K key, Function<V, V> modifier, Supplier<V> newCreator, boolean createIfNotExists, boolean supportRecursiveCalls) {
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
