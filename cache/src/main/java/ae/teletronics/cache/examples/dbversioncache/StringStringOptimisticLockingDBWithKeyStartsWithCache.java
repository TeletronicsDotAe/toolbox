package ae.teletronics.cache.examples.dbversioncache;

import java.util.HashMap;
import java.util.Map;

import ae.teletronics.cache.ChangingValueAndLevelMultiCache;
import ae.teletronics.cache.Pair;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.cache.CacheBuilder;

public class StringStringOptimisticLockingDBWithKeyStartsWithCache extends KeyValueOptimisticLockingDBWithPluggableCache<String, String, StringValueContainer> {
	
	public static class CacheValue {
		
		private boolean complete;
		private Map<String, StringValueContainer> keySuffixToValueMap;
		
		public CacheValue() {
			complete = false;
			keySuffixToValueMap = new HashMap<String, StringValueContainer>();
		}

		public boolean isComplete() {
			return complete;
		}
		
		public void setComplete() {
			complete = true;
		}

		public Map<String, StringValueContainer> getKeySuffixToValueMap() {
			return keySuffixToValueMap;
		}
		
	}

	public class KeyStartsWithCache implements Cache<String, String, StringValueContainer> {
		
		private static final String SPLIT = "!";
		
		private final ChangingValueAndLevelMultiCache<String, CacheValue> innerCache;
		
		private KeyStartsWithCache(int cachesSize, int[] levelSplitAfter) {
			com.google.common.cache.Cache<String, CacheValue> innerInnerCache = CacheBuilder.newBuilder().maximumSize(cachesSize).build();
			ChangingValueAndLevelMultiCache.Builder<String, CacheValue> innerCacheBuilder = ChangingValueAndLevelMultiCache.builder();  
			innerCacheBuilder
					.cache(innerInnerCache)
					.defaultModifier(new Function<CacheValue, CacheValue>() {

						@Override
						public CacheValue apply(CacheValue input) {
							return input;
						}
						
					})
					.levelCalculator(new ChangingValueAndLevelMultiCache.BiFunction<String, CacheValue, Integer>() {
	
						@Override
						public Integer apply(String key, CacheValue cacheValue) {
							return cacheValue.getKeySuffixToValueMap().size();
						}
					
					});
			int currentLevelIntervalStart = 0;
			for (int currentLevelIntervalEnd : levelSplitAfter) {
				innerInnerCache = CacheBuilder.newBuilder().maximumSize(cachesSize).build();
				innerCacheBuilder.addCache(innerInnerCache, currentLevelIntervalStart, currentLevelIntervalEnd, "Inner Cache " + currentLevelIntervalStart + "-" + currentLevelIntervalEnd + " size");
				currentLevelIntervalStart = currentLevelIntervalEnd+1;
			}

			innerCache = innerCacheBuilder.build();
		}

		@Override
		public void put(final String key, final StoreRequest<String, StringValueContainer> storeRequest) throws AlreadyExistsException, DoesNotAlreadyExistException, VersionConflictException {
			put(key, storeRequest, false);
		}
		
		protected void put(final String key, final StoreRequest<String, StringValueContainer> storeRequest, final boolean putInStore) throws AlreadyExistsException, DoesNotAlreadyExistException, VersionConflictException {
			try {
				final Pair<String, String> splittedKey = splitKey(key);
				innerCache.modify(splittedKey._1,
						new Supplier<CacheValue>() {
							
							@Override
							public CacheValue get() {
								return new CacheValue();
							}
							
						},
						new Function<CacheValue, CacheValue>() {
	
							@Override
							public CacheValue apply(CacheValue input) {
								try {
									// Taking advantage of the fact that the cache itself is synchronizing on key - usable if we also just add to store in that synch block
									StringValueContainer newValue = versionCheck(key, storeRequest);
									if (putInStore) store.put(key, newValue);
									input.getKeySuffixToValueMap().put(splittedKey._2, newValue);
									return input;
								} catch (Exception e) {
									throw (e instanceof RuntimeException)?((RuntimeException)e):new RuntimeException(e);
								}
							}
					
						}, true);
			} catch (RuntimeException e) {
				Throwable cause = e.getCause();
				if (cause instanceof AlreadyExistsException) throw (AlreadyExistsException)cause;
				if (cause instanceof DoesNotAlreadyExistException) throw (DoesNotAlreadyExistException)cause;
				if (cause instanceof VersionConflictException) throw (VersionConflictException)cause;
				throw e;
			}
		}

		@Override
		public StringValueContainer get(String key) {
			final Pair<String, String> splittedKey = splitKey(key);
			CacheValue cacheValue = innerCache.getIfPresent(splittedKey._1);
			if (cacheValue != null) return cacheValue.getKeySuffixToValueMap().get(splittedKey._2);
			return null;
		}
		
		@Override
		public Long getVersion(String key) {
			StringValueContainer valueContainer = get(key);
			return (valueContainer != null)?valueContainer.getVersion():null;
		}
		
		private Pair<String, String> splitKey(String key) {
			int splitIndex = key.indexOf(SPLIT);
			if (splitIndex < 0) return new Pair<String, String>(key, null);
			return new Pair<String, String>(key.substring(0, splitIndex), key.substring(splitIndex + SPLIT.length()));
		}

	}
	
	public StringStringOptimisticLockingDBWithKeyStartsWithCache(int cacheSize, int[] levelSplitAfter) {
		super();
		initialize(new KeyStartsWithCache(cacheSize, levelSplitAfter));
	}
	
	@Override
	// Taking advantage of the fact that the cache itself is synchronizing on key - usable if we also just add to store in that synch block
	protected Object getSynchObject(String key) {
		return null;
	}
	
	@Override
	protected void putImpl(String key, StoreRequest<String, StringValueContainer> storeRequest) throws AlreadyExistsException, DoesNotAlreadyExistException, VersionConflictException {
		// Taking advantage of the fact that the cache itself is synchronizing on key - usable if we also just add to store in that synch block
		((KeyStartsWithCache)cache).put(key, storeRequest, true);
	}
	
	public Map<String, StringValueContainer> getAllWithKeyStartingWith(final String keyStart) {
		CacheValue cacheValue = ((KeyStartsWithCache)cache).innerCache.modify(keyStart, 
				new Supplier<CacheValue>() {

					@Override
					public CacheValue get() {
						CacheValue newCacheValue = new CacheValue();
						newCacheValue.getKeySuffixToValueMap().putAll(getAllFromStore(keyStart, null));
						newCacheValue.setComplete();
						return newCacheValue;
					}
			
				},
				new Function<CacheValue, CacheValue>() {

					@Override
					public CacheValue apply(CacheValue input) {
						if (!input.isComplete()) {
							input.getKeySuffixToValueMap().putAll(getAllFromStore(keyStart, input.getKeySuffixToValueMap()));
							input.setComplete();
						}
						return input;
					}
					
				}, true);
		// Simulate that the client does not receive the same object as the database tries to return to it
		// because the object is sent over a network
		return cloneMapStringStringValueContainer(cacheValue.getKeySuffixToValueMap());
	}
	
	private Map<String, StringValueContainer> getAllFromStore(String keyStart, Map<String, StringValueContainer> dontGet) {
		Map<String, StringValueContainer> result = new HashMap<String, StringValueContainer>();
		for (Map.Entry<String, StringValueContainer> entry : store.entrySet()) {
			Pair<String, String> splittedEntryKey = ((KeyStartsWithCache)cache).splitKey(entry.getKey());
			if (keyStart.equals(splittedEntryKey._1) && (dontGet == null || !dontGet.containsKey(splittedEntryKey._2))) {
				result.put(splittedEntryKey._2, entry.getValue());
			}
		}
		return result;
	}
	
	private Map<String, StringValueContainer> cloneMapStringStringValueContainer(Map<String, StringValueContainer> toBeCloned) {
		if (toBeCloned == null) return null;
		Map<String, StringValueContainer> clone = new HashMap<String, StringValueContainer>(toBeCloned.size());
		for (Map.Entry<String, StringValueContainer> entry : toBeCloned.entrySet()) {
			clone.put(new String(entry.getKey()), entry.getValue().clone());
		}
		return clone;
	}
 
}
