package ae.teletronics.cache.examples.dbversioncache;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.google.common.collect.Interner;
import com.google.common.collect.Interners;

public class KeyValueOptimisticLockingDBWithPluggableCache<K, VV, V extends KeyValueOptimisticLockingDBWithPluggableCache.Value<VV>> {
	
	public static class AlreadyExistsException extends Exception {
		private static final long serialVersionUID = 1L;
	}
	
	public static class DoesNotAlreadyExistException extends Exception {
		private static final long serialVersionUID = 1L;
	}
	
	public static class VersionConflictException extends Exception {
		private static final long serialVersionUID = 1L;
	}
	
	public interface Value<V> {
		
		void setVersion(Long newVersion);
		
		Long getVersion();
		
		V getValue();
		
	}
	
	public interface StoreRequest<VV, V extends Value<VV>> {
		
		enum Operation {
			NEW,
			UPDATE
		}
		
		V getValue();
		
		Operation getRequestedOperation();
		
	}

	
	// Pretend that the database actually use a store on disk, making it expensive
	// to fetch values
	protected ConcurrentMap<K, V> store = new ConcurrentHashMap<K, V>();
	// Much less expensive to fetch values from cache
	protected Cache<K, VV, V> cache;
	
	private final Interner<Integer> keyInterner;
	
	public KeyValueOptimisticLockingDBWithPluggableCache() {
		keyInterner = Interners.newWeakInterner();
	}
	
	public void initiatlize(Cache<K, VV, V> cache) {
		this.cache = cache;		
	}
	
	public void put(K key, StoreRequest<VV, V> storeRequest) throws AlreadyExistsException, DoesNotAlreadyExistException, VersionConflictException {
		Object synchObject = getSynchObject(key);
		if (synchObject != null) {
			synchronized(synchObject) {
				putImpl(key, storeRequest);
			}
		} else {
			putImpl(key, storeRequest);
		}
	}
	
	protected Object getSynchObject(K key) {
		return keyInterner.intern(key.hashCode());
	}
	
	protected void putImpl(K key, StoreRequest<VV, V> storeRequest) throws AlreadyExistsException, DoesNotAlreadyExistException, VersionConflictException {
		V newValue = versionCheck(key, storeRequest);
		store.put(key, newValue);
		cache.put(key, storeRequest);
	}
	
	protected final V versionCheck(K key, StoreRequest<VV, V> storeRequest) throws AlreadyExistsException, DoesNotAlreadyExistException, VersionConflictException {
		Long currentVersion = getCurrentVersion(key);
		if ((storeRequest.getRequestedOperation() == StoreRequest.Operation.NEW) &&	(currentVersion != null)) {
			throw new AlreadyExistsException();
		}
		if (storeRequest.getRequestedOperation() == StoreRequest.Operation.UPDATE) {
			if (currentVersion == null) {
				throw new DoesNotAlreadyExistException();
			} else if (!storeRequest.getValue().getVersion().equals(currentVersion)) {
				throw new VersionConflictException();
			}
		}
		V newValue = storeRequest.getValue();
		newValue.setVersion((storeRequest.getRequestedOperation() == StoreRequest.Operation.NEW)?0:(newValue.getVersion()+1));
		return newValue;
	}
	
	public V get(K key) {
		V cacheValue = cache.get(key);
		if (cacheValue != null) {
			return cacheValue;
		} else {
			final V storeValue = store.get(key);
			try {
				if (storeValue != null) cache.put(key, new StoreRequest<VV, V>() {
					
					@Override
					public V getValue() {
						return storeValue;
					}
	
					@Override
					public StoreRequest.Operation getRequestedOperation() {
						return StoreRequest.Operation.NEW;
					}
					
				});
			} catch (Exception e) {
				// ignore
			}
			return storeValue;
		}
	}
	
	private Long getCurrentVersion(K key) {
		Long currentVersion = cache.getVersion(key);
		if (currentVersion != null) return currentVersion;
		V currentValue = get(key);
		return (currentValue != null)?currentValue.getVersion():null;
	}
	
	public interface Cache<K, V, VV extends KeyValueOptimisticLockingDBWithPluggableCache.Value<V>> {
		
		void put(K key, StoreRequest<V, VV> storeRequest) throws AlreadyExistsException, DoesNotAlreadyExistException, VersionConflictException;
		
		VV get(K key);
		
		Long getVersion(K key);
	}

}
