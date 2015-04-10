package ae.teletronics.cache.examples.dbversioncache;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.google.common.collect.Interner;
import com.google.common.collect.Interners;

public class KeyValueOptimisticLockingDBWithPluggableCache<STOREKEY, STOREVALUE, STOREVALUECONTAINER extends KeyValueOptimisticLockingDBWithPluggableCache.ValueContainer<STOREVALUE>> {
	
	public static class AlreadyExistsException extends Exception {
		private static final long serialVersionUID = 1L;
	}
	
	public static class DoesNotAlreadyExistException extends Exception {
		private static final long serialVersionUID = 1L;
	}
	
	public static class VersionConflictException extends Exception {
		private static final long serialVersionUID = 1L;
	}
	
	public interface ValueContainer<VALUE> extends Cloneable {
		
		void setVersion(Long newVersion);
		
		Long getVersion();
		
		VALUE getValue();
		
		ValueContainer<VALUE> clone();
		
	}
	
	public interface StoreRequest<VALUE, VALUECONTAINER extends ValueContainer<VALUE>> {
		
		enum Operation {
			NEW,
			UPDATE
		}
		
		VALUECONTAINER getValueContainer();
		
		Operation getRequestedOperation();
		
	}

	
	// Pretend that the database actually use a store on disk, making it expensive
	// to fetch values
	protected ConcurrentMap<STOREKEY, STOREVALUECONTAINER> store = new ConcurrentHashMap<STOREKEY, STOREVALUECONTAINER>();
	// Much less expensive to fetch values from cache
	protected Cache<STOREKEY, STOREVALUE, STOREVALUECONTAINER> cache;
	
	private final Interner<Integer> keyInterner;
	
	public KeyValueOptimisticLockingDBWithPluggableCache() {
		keyInterner = Interners.newWeakInterner();
	}
	
	public void initialize(Cache<STOREKEY, STOREVALUE, STOREVALUECONTAINER> cache) {
		this.cache = cache;		
	}
	
	public void put(STOREKEY key, StoreRequest<STOREVALUE, STOREVALUECONTAINER> storeRequest) throws AlreadyExistsException, DoesNotAlreadyExistException, VersionConflictException {
		Object synchObject = getSynchObject(key);
		if (synchObject != null) {
			synchronized(synchObject) {
				putImpl(key, storeRequest);
			}
		} else {
			putImpl(key, storeRequest);
		}
	}
	
	protected Object getSynchObject(STOREKEY key) {
		return keyInterner.intern(key.hashCode());
	}
	
	protected void putImpl(STOREKEY key, StoreRequest<STOREVALUE, STOREVALUECONTAINER> storeRequest) throws AlreadyExistsException, DoesNotAlreadyExistException, VersionConflictException {
		STOREVALUECONTAINER newValue = versionCheck(key, storeRequest);
		store.put(key, newValue);
		cache.put(key, storeRequest);
	}
	
	protected final STOREVALUECONTAINER versionCheck(STOREKEY key, StoreRequest<STOREVALUE, STOREVALUECONTAINER> storeRequest) throws AlreadyExistsException, DoesNotAlreadyExistException, VersionConflictException {
		Long currentVersion = getCurrentVersion(key);
		if ((storeRequest.getRequestedOperation() == StoreRequest.Operation.NEW) &&	(currentVersion != null)) {
			throw new AlreadyExistsException();
		}
		if (storeRequest.getRequestedOperation() == StoreRequest.Operation.UPDATE) {
			if (currentVersion == null) {
				throw new DoesNotAlreadyExistException();
			} else if (!storeRequest.getValueContainer().getVersion().equals(currentVersion)) {
				throw new VersionConflictException();
			}
		}
		STOREVALUECONTAINER newValue = storeRequest.getValueContainer();
		newValue.setVersion((storeRequest.getRequestedOperation() == StoreRequest.Operation.NEW)?0:(newValue.getVersion()+1));
		return newValue;
	}
	
	public STOREVALUECONTAINER get(STOREKEY key) {
		return get(key, true);
	}
	
	private STOREVALUECONTAINER get(STOREKEY key, boolean putInCache) {
		STOREVALUECONTAINER cacheValue = cache.get(key);
		STOREVALUECONTAINER returnValue;
		if (cacheValue != null) {
			returnValue = cacheValue;
		} else {
			final STOREVALUECONTAINER storeValue = store.get(key);
			if (storeValue != null && putInCache) {
				try {
					
						cache.put(key, new StoreRequest<STOREVALUE, STOREVALUECONTAINER>() {
							
							@Override
							public STOREVALUECONTAINER getValueContainer() {
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
			}
			
			returnValue = storeValue;
		}
		// Simulate that the client does not receive the same object as the database tries to return to it
		// because the object is sent over a network
		@SuppressWarnings("unchecked")
		STOREVALUECONTAINER clone = (returnValue != null)?(STOREVALUECONTAINER)returnValue.clone():null;
		return clone;
	}
	
	private Long getCurrentVersion(STOREKEY key) {
		Long currentVersion = cache.getVersion(key);
		if (currentVersion != null) return currentVersion;
		STOREVALUECONTAINER currentValue = get(key, false);
		return (currentValue != null)?currentValue.getVersion():null;
	}
	
	public interface Cache<STOREKEY, STOREVALUE, STOREVALUECONTAINER extends KeyValueOptimisticLockingDBWithPluggableCache.ValueContainer<STOREVALUE>> {
		
		void put(STOREKEY key, StoreRequest<STOREVALUE, STOREVALUECONTAINER> storeRequest) throws AlreadyExistsException, DoesNotAlreadyExistException, VersionConflictException;
		
		STOREVALUECONTAINER get(STOREKEY key);
		
		Long getVersion(STOREKEY key);
	}

}
