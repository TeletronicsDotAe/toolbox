package ae.teletronics.cache.examples.dbversioncache;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.google.common.collect.Interner;
import com.google.common.collect.Interners;

public class KeyValueOptimisticLockingDBWithPluggableCache<K, V, VV extends KeyValueOptimisticLockingDBWithPluggableCache.Value<V>> {
	
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
	
	public interface StoreRequest<V, VV extends Value<V>> {
		
		enum Operation {
			NEW,
			UPDATE
		}
		
		VV getValue();
		
		Operation getRequestedOperation();
		
	}

	
	// Pretend that the database actually use a store on disk, making it expensive
	// to fetch values
	protected ConcurrentMap<K, VV> store = new ConcurrentHashMap<K, VV>();
	// Much less expensive to fetch values from cache
	protected Cache<K, V, VV> cache;
	
	private final Interner<Integer> keyInterner;
	
	public KeyValueOptimisticLockingDBWithPluggableCache() {
		keyInterner = Interners.newWeakInterner();
	}
	
	public void initiatlize(Cache<K, V, VV> cache) {
		this.cache = cache;		
	}
	
	public void put(K key, StoreRequest<V, VV> storeRequest) throws AlreadyExistsException, DoesNotAlreadyExistException, VersionConflictException {
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
	
	protected void putImpl(K key, StoreRequest<V, VV> storeRequest) throws AlreadyExistsException, DoesNotAlreadyExistException, VersionConflictException {
		VV newValue = versionCheck(key, storeRequest);
		store.put(key, newValue);
		cache.put(key, storeRequest);
	}
	
	protected final VV versionCheck(K key, StoreRequest<V, VV> storeRequest) throws AlreadyExistsException, DoesNotAlreadyExistException, VersionConflictException {
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
		VV newValue = storeRequest.getValue();
		newValue.setVersion((storeRequest.getRequestedOperation() == StoreRequest.Operation.NEW)?0:(newValue.getVersion()+1));
		return newValue;
	}
	
	public VV get(K key) {
		VV cacheValue = cache.get(key);
		return (cacheValue != null)?cacheValue:store.get(key);
	}
	
	private Long getCurrentVersion(K key) {
		Long currentVersion = cache.getVersion(key);
		if (currentVersion != null) return currentVersion;
		VV currentValue = get(key);
		return (currentValue != null)?currentValue.getVersion():null;
	}
	
	public interface Cache<K, V, VV extends KeyValueOptimisticLockingDBWithPluggableCache.Value<V>> {
		
		void put(K key, StoreRequest<V, VV> storeRequest) throws AlreadyExistsException, DoesNotAlreadyExistException, VersionConflictException;
		
		VV get(K key);
		
		Long getVersion(K key);
	}

}
