package ae.teletronics.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;



// TODO java8 import java.util.function.Function;
// TODO java8 import java.util.function.Predicate;
// TODO java8 import java.util.function.Supplier;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.cache.Cache;
import com.google.common.collect.Interner;
import com.google.common.collect.Interners;

public class ChangingValueCache<K, V> {
	
	public static class Builder<K, V> {
		
		protected final ChangingValueCache<K, V> instance;
		
		protected Builder() {
			instance = createInstance();
		}
		
		protected ChangingValueCache<K, V> createInstance() {
			return new ChangingValueCache<K, V>();
		}
		
		public Builder<K, V> defaultNewCreator(Supplier<V> newCreator) {
			instance.defaultNewCreator = newCreator;
			return this;
		}
		
		public Builder<K, V> defaultModifier(Function<V, V> modifier) {
			instance.defaultModifier = modifier;
			return this;
		}
		
		public Builder<K, V> cache(Cache<K, V> cache) {
			instance.cache = cache;
			return this;
		}
		
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
	
	public static <K, V> Builder<K, V> builder() {
	    return new Builder<K, V>();
	}
	
	public final V modify(K key, boolean createIfNotExists) {
		return modify(key, null, null, createIfNotExists);
	}
	
	public final V modify(K key, Function<V, V> modifier, boolean createIfNotExists) {
		return modify(key, modifier, null, createIfNotExists);
	}

	public final V modify(K key, Supplier<V> newCreator, boolean createIfNotExists) {
		return modify(key, null, newCreator, createIfNotExists);
	}
	
	public final V modify(K key, Function<V, V> modifier, Supplier<V> newCreator, boolean createIfNotExists) {
		return modify(key, modifier, newCreator, createIfNotExists, false);
	}
	
	public final V modify(K key, Function<V, V> modifier, Supplier<V> newCreator, boolean createIfNotExists, boolean supportRecursiveCalls) {
		synchronized(newOrMovingCacheEntryInterner.intern(key.hashCode())) {
			return modifyImpl(key, modifier, newCreator, createIfNotExists, supportRecursiveCalls);
		}
	}
	
	public final void modifyAll(Predicate<K> keyPredicate, Predicate<V> valuePredicate, boolean createIfNotExists) {
		modifyAll(keyPredicate, valuePredicate, null, createIfNotExists);
	}
	
	public final void modifyAll(Predicate<K> keyPredicate, Predicate<V> valuePredicate, Function<V, V> modifier, boolean createIfNotExists) {
		modifyAll(keyPredicate, valuePredicate, modifier, createIfNotExists, false);
	}
	
	public final void modifyAll(Predicate<K> keyPredicate, Predicate<V> valuePredicate, Function<V, V> modifier, boolean createIfNotExists, boolean supportRecursiveCalls) {
		for (Cache<K, V> cache : getAllCaches()) {
			for (Map.Entry<K, V> entry : cache.asMap().entrySet()) {
				if ((keyPredicate == null || keyPredicate.apply(entry.getKey())) &&
					(valuePredicate == null || valuePredicate.apply(entry.getValue()))) {
					modify(entry.getKey(), modifier, null, createIfNotExists, supportRecursiveCalls);
				}
			}
		}
	}
	
	protected ThreadLocal<V> alreadyWorkingOn = new ThreadLocal<V>();
	protected V modifyImpl(K key, Function<V, V> modifier, Supplier<V> newCreator, boolean createIfNotExists, boolean supportRecursiveCalls) {
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
	
	public V getIfPresent(K key) {
		for (Cache<K, V> cache : getAllCaches()) {
			V value = cache.getIfPresent(key);
			if (value != null) return value;
		}
		return null;
	}
	
	public long size() {
		long size = 0;
		for (Cache<K, V> cache : getAllCaches()) {
			size += cache.size();
		}
		return size;
	}
	
	public V getAddIfNotPresent(K key) {
		return getAddIfNotPresent(key, null);
	}
	
	public V getAddIfNotPresent(K key, Supplier<V> newCreator) {
		return modify(key, newCreator, true);
	}

}
