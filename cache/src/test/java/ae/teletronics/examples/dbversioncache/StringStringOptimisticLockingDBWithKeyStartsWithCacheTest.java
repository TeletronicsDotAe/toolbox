package ae.teletronics.examples.dbversioncache;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.cache.Cache;

import ae.teletronics.cache.ChangingValueAndLevelMultiCache;
import ae.teletronics.cache.Pair;
import ae.teletronics.cache.examples.dbversioncache.KeyValueOptimisticLockingDBWithPluggableCache.AlreadyExistsException;
import ae.teletronics.cache.examples.dbversioncache.KeyValueOptimisticLockingDBWithPluggableCache.DoesNotAlreadyExistException;
import ae.teletronics.cache.examples.dbversioncache.KeyValueOptimisticLockingDBWithPluggableCache.StoreRequest;
import ae.teletronics.cache.examples.dbversioncache.KeyValueOptimisticLockingDBWithPluggableCache.VersionConflictException;
import ae.teletronics.cache.examples.dbversioncache.StringStringOptimisticLockingDBWithKeyStartsWithCache.CacheValue;
import ae.teletronics.cache.examples.dbversioncache.StringStringOptimisticLockingDBWithKeyStartsWithCache;
import ae.teletronics.cache.examples.dbversioncache.StringValueContainer;

public class StringStringOptimisticLockingDBWithKeyStartsWithCacheTest {

	private static final int CACHES_SIZE = 5;
	private static final int[] LEVEL_SPLIT_AFTER = new int[]{2, Integer.MAX_VALUE};
	private static final String SPLIT = "!";

	private ExposingStringStringOptimisticLockingDBWithKeyStartsWithCache underTest;
	
	@Before
	public void before() {
		underTest = new ExposingStringStringOptimisticLockingDBWithKeyStartsWithCache(CACHES_SIZE, LEVEL_SPLIT_AFTER);
	}
	
	private class ExposingStringStringOptimisticLockingDBWithKeyStartsWithCache extends StringStringOptimisticLockingDBWithKeyStartsWithCache {

		private ExposingStringStringOptimisticLockingDBWithKeyStartsWithCache(int cacheSize, int[] levelSplitAfter) {
			super(cacheSize, levelSplitAfter);
		}
		
		private StringStringOptimisticLockingDBWithKeyStartsWithCache.KeyStartsWithCache getCache() {
			return (StringStringOptimisticLockingDBWithKeyStartsWithCache.KeyStartsWithCache)cache;
		}
		
		private ChangingValueAndLevelMultiCache<String, CacheValue> getInnerCache() {
			try {
				Field innerCacheField = StringStringOptimisticLockingDBWithKeyStartsWithCache.KeyStartsWithCache.class.getDeclaredField("innerCache");
				innerCacheField.setAccessible(true);
				@SuppressWarnings("unchecked")
				ChangingValueAndLevelMultiCache<String, CacheValue> result = (ChangingValueAndLevelMultiCache<String, CacheValue>)innerCacheField.get(getCache());
				return result;
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
		
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testConcurrency() {
		// for X in 1, 2 and 3 (should go to level-two because they have more than 2 key-suffixes)
		// X!1=abc, X!2=bcd, X!3=cde, X!4=def, X!5=efg
		// for X in 4, 5, 6, 7, 8, 9, 10, 11, 12 and 13 (should go to level-one because they have 2 (or less) key-suffixes - but they do not all fit there, some will be evicted)
		// X!1=abc, X!2=bcd
		Map<String, Pair<String[], String[]>[]> LETTER_TO_PREFIX_STR_SUFFIX_STR_MAP = new HashMap<String, Pair<String[], String[]>[]>();
		LETTER_TO_PREFIX_STR_SUFFIX_STR_MAP.put("a", new Pair[]{new Pair<String[], String[]>(new String[]{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13"}, new String[]{"1"})});
		LETTER_TO_PREFIX_STR_SUFFIX_STR_MAP.put("b", new Pair[]{new Pair<String[], String[]>(new String[]{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13"}, new String[]{"1", "2"})});
		LETTER_TO_PREFIX_STR_SUFFIX_STR_MAP.put("c", new Pair[]{new Pair<String[], String[]>(new String[]{"1", "2", "3"}, new String[]{"1", "2", "3"}), new Pair<String[], String[]>(new String[]{"4", "5", "6", "7", "8", "9", "10", "11", "12", "13"}, new String[]{"1", "2"})});
		LETTER_TO_PREFIX_STR_SUFFIX_STR_MAP.put("d", new Pair[]{new Pair<String[], String[]>(new String[]{"1", "2", "3"}, new String[]{"2", "3", "4"}), new Pair<String[], String[]>(new String[]{"4", "5", "6", "7", "8", "9", "10", "11", "12", "13"}, new String[]{"2"})});
		LETTER_TO_PREFIX_STR_SUFFIX_STR_MAP.put("e", new Pair[]{new Pair<String[], String[]>(new String[]{"1", "2", "3"}, new String[]{"3", "4", "5"})});
		LETTER_TO_PREFIX_STR_SUFFIX_STR_MAP.put("f", new Pair[]{new Pair<String[], String[]>(new String[]{"1", "2", "3"}, new String[]{"4", "5"})});
		LETTER_TO_PREFIX_STR_SUFFIX_STR_MAP.put("g", new Pair[]{new Pair<String[], String[]>(new String[]{"1", "2", "3"}, new String[]{"5"})});
		String[] NUMBERS_IN_LEVEL_TWO_CACHE = new String[]{"1", "2", "3"};
		String[] NUMBERS_POTENTIALLY_IN_LEVEL_ONE_CACHE = new String[]{"4", "5", "6", "7", "8", "9", "10", "11", "12", "13"};
		int MIN_NUMBERS_IN_LEVEL_ONE_CACHE = CACHES_SIZE - NUMBERS_IN_LEVEL_TWO_CACHE.length;
		
		List<Runnable> tasks = new ArrayList<Runnable>();
		for (Map.Entry<String, Pair<String[], String[]>[]> entry : LETTER_TO_PREFIX_STR_SUFFIX_STR_MAP.entrySet()) {
			String letter = entry.getKey();
			for (Pair<String[], String[]> prefixStrSuffixStr : entry.getValue()) {
				tasks.add(new MyRunnable(prefixStrSuffixStr._1, prefixStrSuffixStr._2, letter));
			}
		}

		Thread[] threads = new Thread[tasks.size()];
		for (int i = 0; i < threads.length; i++) {
			threads[i] = new Thread(tasks.get(i));
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
		
		Cache<String, CacheValue> levelTwoCache = null;
		for (String number : NUMBERS_IN_LEVEL_TWO_CACHE) {
			Pair<Cache<String, CacheValue>, CacheValue> cacheAndValue = underTest.getInnerCache().getCacheAndValueIfPresent(number);
			Assert.assertNotNull(cacheAndValue);
			Assert.assertNotNull(cacheAndValue._1);
			if (levelTwoCache == null) {
				levelTwoCache = cacheAndValue._1;
				Assert.assertEquals(NUMBERS_IN_LEVEL_TWO_CACHE.length, levelTwoCache.size());
			} else {
				Assert.assertEquals(levelTwoCache, cacheAndValue._1);
			}
			CacheValue cacheValue = cacheAndValue._2;
			Assert.assertNotNull(cacheValue);
			Map<String, StringValueContainer> map = cacheValue.getKeySuffixToValueMap();
			Assert.assertEquals(5, map.size());
			
			StringValueContainer svc = map.get("1");
			String value = svc.getValue();
			Assert.assertTrue(value, value.contains("a"));
			Assert.assertTrue(value, value.contains("b"));
			Assert.assertTrue(value, value.contains("c"));
			
			svc = map.get("2");
			value = svc.getValue();
			Assert.assertTrue(value, value.contains("b"));
			Assert.assertTrue(value, value.contains("c"));
			Assert.assertTrue(value, value.contains("d"));
			
			svc = map.get("3");
			value = svc.getValue();
			Assert.assertTrue(value, value.contains("c"));
			Assert.assertTrue(value, value.contains("d"));
			Assert.assertTrue(value, value.contains("e"));
			
			svc = map.get("4");
			value = svc.getValue();
			Assert.assertTrue(value, value.contains("d"));
			Assert.assertTrue(value, value.contains("e"));
			Assert.assertTrue(value, value.contains("f"));
			
			svc = map.get("5");
			value = svc.getValue();
			Assert.assertTrue(value, value.contains("e"));
			Assert.assertTrue(value, value.contains("f"));
			Assert.assertTrue(value, value.contains("g"));
		}

		Cache<String, CacheValue> levelOneCache = null;
		int foundInLevelOneCache = 0;
		for (String number : NUMBERS_POTENTIALLY_IN_LEVEL_ONE_CACHE) {
			Pair<Cache<String, CacheValue>, CacheValue> cacheAndValue = underTest.getInnerCache().getCacheAndValueIfPresent(number);
			if (cacheAndValue != null) {
				foundInLevelOneCache++;
				Assert.assertNotNull(cacheAndValue._1);
				if (levelOneCache == null) {
					levelOneCache = cacheAndValue._1;
				} else {
					Assert.assertEquals(levelOneCache, cacheAndValue._1);
				}
				CacheValue cacheValue = cacheAndValue._2;
				Assert.assertNotNull(cacheValue);
				Map<String, StringValueContainer> map = cacheValue.getKeySuffixToValueMap();
				Assert.assertEquals(2, map.size());
				
				StringValueContainer svc = map.get("1");
				String value = svc.getValue();
				Assert.assertTrue(value, value.contains("a"));
				Assert.assertTrue(value, value.contains("b"));
				Assert.assertTrue(value, value.contains("c"));
				
				svc = map.get("2");
				value = svc.getValue();
				Assert.assertTrue(value, value.contains("b"));
				Assert.assertTrue(value, value.contains("c"));
				Assert.assertTrue(value, value.contains("d"));
			}
		}
		// Everything in level-one cache is from NUMBERS_POTENTIALLY_IN_LEVEL_ONE_CACHE
		Assert.assertTrue(levelOneCache.size() + ", " + foundInLevelOneCache, levelOneCache.size() == foundInLevelOneCache);
		// At least MIN_NUMBERS_IN_LEVEL_ONE_CACHE from NUMBERS_POTENTIALLY_IN_LEVEL_ONE_CACHE are actually in cache
		Assert.assertTrue("" + levelOneCache.size(), levelOneCache.size() >= MIN_NUMBERS_IN_LEVEL_ONE_CACHE);

		// Now assert correctness - independent of in-cache or not
		for (Map.Entry<String, Pair<String[], String[]>[]> entry : LETTER_TO_PREFIX_STR_SUFFIX_STR_MAP.entrySet()) {
			String letter = entry.getKey();
			for (Pair<String[], String[]> prefixStrSuffixStr : entry.getValue()) {
				for (String prefixStr : prefixStrSuffixStr._1) {
					for (String suffixStr : prefixStrSuffixStr._2) {
						String key = prefixStr + SPLIT + suffixStr;
						StringValueContainer svc = underTest.get(key);
						String value = svc.getValue();
						Assert.assertTrue(key + ", " + value, value.contains(letter));
					}
				}
			}
		}
	}
	
	private class MyRunnable implements Runnable {

		private final String[] keyPrefixes;
		private final String[] keySuffixes;
		private final String toAdd;
		
		private MyRunnable(String[] keyPrefixes, String[] keySuffixes, String toAdd) {
			this.keyPrefixes = keyPrefixes;
			this.keySuffixes = keySuffixes;
			this.toAdd = toAdd;
		}

		@Override
		public void run() {
			for (String keyPrefix : keyPrefixes) {
				Map<String, StringValueContainer> keySuffixToValueContainerMap = underTest.getAllWithKeyStartingWith(keyPrefix);
				for (String keySuffix : keySuffixes) {
					String key = keyPrefix + SPLIT + keySuffix;
					StringValueContainer fetchedValueContainer = keySuffixToValueContainerMap.get(keySuffix);
					if (fetchedValueContainer == null || !fetchedValueContainer.getValue().contains(toAdd)) {
						boolean firstRound = true;
						boolean doesNotAlreadyExistSeen = false;
						do {
							final StringValueContainer valueContainer =
									(firstRound)?fetchedValueContainer:
									((doesNotAlreadyExistSeen)?null:underTest.get(key));
							firstRound = false;
							doesNotAlreadyExistSeen = false;
							try {
								underTest.put(key, new StoreRequest<String, StringValueContainer>() {
			
									@Override
									public StringValueContainer getValueContainer() {
										return new StringValueContainer((valueContainer != null)?valueContainer.getVersion():-1, (valueContainer != null)?(valueContainer.getValue() + toAdd):toAdd);
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

	}
	
}
