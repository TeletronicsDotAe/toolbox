package ae.teletronics.cache.examples.dbversioncache;

import ae.teletronics.cache.ChangingValueCache;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.cache.CacheBuilder;

public class StringStringOptimisticLockingDBWithVersionCache extends KeyValueOptimisticLockingDBWithPluggableCache<String, String, StringStringOptimisticLockingDBWithVersionCache.Value> {
	
	public static class Value implements KeyValueOptimisticLockingDBWithPluggableCache.Value<String> {
		
		private Long version;
		private String text;
		
		public Value(Long version, String text) {
			this.version = version;
			this.text = text;
		}

		@Override
		public void setVersion(Long newVersion) {
			version = newVersion;
		}

		@Override
		public Long getVersion() {
			return version;
		}

		@Override
		public String getValue() {
			return text;
		}
		
	}

	private class VersionCache implements Cache<String, String, Value> {
		
		private static final long MAX_SIZE = 1000;
		
		private final ChangingValueCache<String, Long> innerCache;
		
		private VersionCache() {
			com.google.common.cache.Cache<String, Long> innerInnerCache = CacheBuilder.newBuilder().maximumSize(MAX_SIZE).build();
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
		public void put(final String key, final StoreRequest<String, Value> storeRequest) throws AlreadyExistsException, DoesNotAlreadyExistException, VersionConflictException {
			put(key, storeRequest, false);
		}
		
		protected void put(final String key, final StoreRequest<String, Value> storeRequest, final boolean putInStore) throws AlreadyExistsException, DoesNotAlreadyExistException, VersionConflictException {
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
									// Taking advantage of the fact that the cache itself in synchronizing on key - usable if we also just add to store in that synch block
									Value newValue = versionCheck(key, storeRequest);
									if (putInStore) store.put(key, newValue);
									return newValue.version;
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
		public Value get(String key) {
			return null;
		}
		
		@Override
		public Long getVersion(String key) {
			return innerCache.getIfPresent(key);
		}
		
	}
	
	public StringStringOptimisticLockingDBWithVersionCache() {
		super();
		initiatlize(new VersionCache());
	}
	
	@Override
	// Taking advantage of the fact that the cache itself in synchronizing on key - usable if we also just add to store in that synch block
	protected Object getSynchObject(String key) {
		return null;
	}
	
	protected void putImpl(String key, StoreRequest<String, Value> storeRequest) throws AlreadyExistsException, DoesNotAlreadyExistException, VersionConflictException {
		// Taking advantage of the fact that the cache itself in synchronizing on key - usable if we also just add to store in that synch block
		((VersionCache)cache).put(key, storeRequest, true);
	}

}
