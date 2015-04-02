package ae.teletronics.examples.dbversioncache;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import ae.teletronics.cache.examples.dbversioncache.KeyValueOptimisticLockingDBWithPluggableCache.AlreadyExistsException;
import ae.teletronics.cache.examples.dbversioncache.KeyValueOptimisticLockingDBWithPluggableCache.DoesNotAlreadyExistException;
import ae.teletronics.cache.examples.dbversioncache.KeyValueOptimisticLockingDBWithPluggableCache.StoreRequest;
import ae.teletronics.cache.examples.dbversioncache.KeyValueOptimisticLockingDBWithPluggableCache.VersionConflictException;
import ae.teletronics.cache.examples.dbversioncache.StringStringOptimisticLockingDBWithVersionCache;
import ae.teletronics.cache.examples.dbversioncache.StringStringOptimisticLockingDBWithVersionCache.Value;

public class StringStringOptimisticLockingDBWithVersionCacheTest {

	private StringStringOptimisticLockingDBWithVersionCache underTest;
	
	@Before
	public void before() {
		underTest = new StringStringOptimisticLockingDBWithVersionCache();
	}
	
	@Test
	public void testConcurrency() {
		final int NO_UPDATES_PER_THREAD = 10;
		final int NO_THREADS = 5;
		
		Runnable task = new Runnable() {

			@Override
			public void run() {
				for (int i = 0; i < NO_UPDATES_PER_THREAD; i++) {
					boolean doesNotAlreadyExistSeen = false;
					do { 
						final Value currentV = (doesNotAlreadyExistSeen)?null:underTest.get("X");
						doesNotAlreadyExistSeen = false;
						try {
							underTest.put("X", new StoreRequest<String, Value>() {
		
								@Override
								public Value getValue() {
									return new Value((currentV != null)?currentV.getVersion():-1, (currentV != null)?(currentV.getValue() + "x"):"x");
								}
		
								@Override
								public StoreRequest.Operation getRequestedOperation() {
									return (currentV != null)?StoreRequest.Operation.UPDATE:StoreRequest.Operation.NEW;
								}
								
							});
						} catch (AlreadyExistsException e) {
							// take another round - get the newest and try update again
							continue;
						} catch (DoesNotAlreadyExistException e) {
							// take another round - but we know that it is not there
							doesNotAlreadyExistSeen = true;
							continue;
						} catch (VersionConflictException e) {
							// take another round - get the newest and try update again
							continue;
						}
						break;
					} while (true);
				}
			}			
		};
		
		Thread[] threads = new Thread[NO_THREADS];
		for (int i = 0; i < threads.length; i++) {
			threads[i] = new Thread(task);
		}
		for (int i = 0; i < threads.length; i++) {
			threads[i].start();
		}
		for (int i = 0; i < threads.length; i++) {
			try {
				threads[i].join();
			} catch (InterruptedException e) {
				// ignore - not gonna happen
			}
		}
		
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < NO_THREADS*NO_UPDATES_PER_THREAD; i++) {
			sb.append("x");
		}
		Assert.assertEquals(sb.toString(), underTest.get("X").getValue());
	}
	
}
