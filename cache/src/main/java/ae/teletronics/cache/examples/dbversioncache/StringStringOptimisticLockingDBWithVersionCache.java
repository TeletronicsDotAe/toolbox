package ae.teletronics.cache.examples.dbversioncache;

import ae.teletronics.cache.ChangingValueCache;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.cache.CacheBuilder;

public class StringStringOptimisticLockingDBWithVersionCache extends KeyValueOptimisticLockingDBWithPluggableCache<String, String, StringValueContainer> {
	
	private class VersionCache implements Cache<String, String, StringValueContainer> {
		
		private final ChangingValueCache<String, Long> innerCache;
		
		private VersionCache(int cacheSize) {
			com.google.common.cache.Cache<String, Long> innerInnerCache = CacheBuilder.newBuilder().maximumSize(cacheSize).build();
			ChangingValueCache.Builder<String, Long> innerCacheBuilder = ChangingValueCache.builder();  
			innerCache = innerCacheBuilder
					.cache(innerInnerCache)
					.defaultModifier(new Function<Long, Long>() {

						@Override
						public Long apply(Long input) {
							return input;
						}
						
					})
					.build();
		}

		@Override
		public void put(final String key, final StoreRequest<String, StringValueContainer> storeRequest) throws AlreadyExistsException, DoesNotAlreadyExistException, VersionConflictException {
			put(key, storeRequest, false);
		}
		
		protected void put(final String key, final StoreRequest<String, StringValueContainer> storeRequest, final boolean putInStore) throws AlreadyExistsException, DoesNotAlreadyExistException, VersionConflictException {
			try {
				innerCache.modify(key,
						new Supplier<Long>() {
							
							@Override
							public Long get() {
								return -1L;
							}
							
						},
						new Function<Long, Long>() {
	
							@Override
							public Long apply(Long input) {
								try {
									// Taking advantage of the fact that the cache itself is synchronizing on key - usable if we also just add to store in that synch block
									StringValueContainer newValue = versionCheck(key, storeRequest);
									if (putInStore) store.put(key, newValue);
									return newValue.getVersion();
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
			return null;
		}
		
		@Override
		public Long getVersion(String key) {
			return innerCache.getIfPresent(key);
		}
		
	}
	
	public StringStringOptimisticLockingDBWithVersionCache(int cacheSize) {
		super();
		initialize(new VersionCache(cacheSize));
	}
	
	@Override
	// Taking advantage of the fact that the cache itself is synchronizing on key - usable if we also just add to store in that synch block
	protected Object getSynchObject(String key) {
		return null;
	}
	
	protected void putImpl(String key, StoreRequest<String, StringValueContainer> storeRequest) throws AlreadyExistsException, DoesNotAlreadyExistException, VersionConflictException {
		// Taking advantage of the fact that the cache itself is synchronizing on key - usable if we also just add to store in that synch block
		((VersionCache)cache).put(key, storeRequest, true);
	}

}
