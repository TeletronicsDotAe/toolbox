package ae.teletronics.cache.examples.dbversioncache;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import ae.teletronics.cache.examples.dbversioncache.KeyValueOptimisticLockingDBWithPluggableCache.AlreadyExistsException;
import ae.teletronics.cache.examples.dbversioncache.KeyValueOptimisticLockingDBWithPluggableCache.DoesNotAlreadyExistException;
import ae.teletronics.cache.examples.dbversioncache.KeyValueOptimisticLockingDBWithPluggableCache.StoreRequest;
import ae.teletronics.cache.examples.dbversioncache.KeyValueOptimisticLockingDBWithPluggableCache.VersionConflictException;
import ae.teletronics.cache.examples.dbversioncache.StringStringOptimisticLockingDBWithVersionCache;
import ae.teletronics.cache.examples.dbversioncache.StringValueContainer;

public class StringStringOptimisticLockingDBWithVersionCacheTest {

	private StringStringOptimisticLockingDBWithVersionCache underTest;
	
	@Before
	public void before() {
		underTest = new StringStringOptimisticLockingDBWithVersionCache(1000);
	}
	
	@Test
	public void testConcurrency() {
		final int NO_UPDATES_PER_THREAD = 10;
		final int NO_THREADS = 5;
		
		Runnable task = new MyRunnable(NO_UPDATES_PER_THREAD);
		
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
	
	private class MyRunnable implements Runnable {
		
		private final int numberOfUpdates;
		
		private MyRunnable(int numberOfUpdates) {
			this.numberOfUpdates = numberOfUpdates;
		}
		
		@Override
		public void run() {
			for (int i = 0; i < numberOfUpdates; i++) {
				boolean doesNotAlreadyExistSeen = false;
				do { 
					final StringValueContainer valueContainer = (doesNotAlreadyExistSeen)?null:underTest.get("X");
					doesNotAlreadyExistSeen = false;
					try {
						underTest.put("X", new StoreRequest<String, StringValueContainer>() {
	
							@Override
							public StringValueContainer getValueContainer() {
								return new StringValueContainer((valueContainer != null)?valueContainer.getVersion():-1, (valueContainer != null)?(valueContainer.getValue() + "x"):"x");
							}
	
							@Override
							public StoreRequest.Operation getRequestedOperation() {
								return (valueContainer != null)?StoreRequest.Operation.UPDATE:StoreRequest.Operation.NEW;
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

	}
	
}
