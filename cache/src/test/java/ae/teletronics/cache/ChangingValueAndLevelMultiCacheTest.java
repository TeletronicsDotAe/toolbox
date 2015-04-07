package ae.teletronics.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

public class ChangingValueAndLevelMultiCacheTest extends ChangingValueCaheTest {
	
	@Override
	protected <V> ChangingValueAndLevelMultiCache.Builder<String, V> createCacheBuilder(Supplier<V> defaultNewCreator, Function<V, V> defaultModifier) {
		Cache<String, V> defaultInnerCache = CacheBuilder.newBuilder().maximumSize(MAX_CACHE_SIZE).build();
		ChangingValueAndLevelMultiCache.Builder<String, V> underTestBuilder = ChangingValueAndLevelMultiCache.builder();
		return underTestBuilder
				.cache(defaultInnerCache)
				.defaultNewCreator(defaultNewCreator)
				.defaultModifier(defaultModifier)
				.levelCalculator(new ChangingValueAndLevelMultiCache.BiFunction<String, V, Integer>() {

					@Override
					public Integer apply(String t, V u) {
						return 1;
					}
					
				});
	}
	
	@Test
	public void testLevelingHeaviestLast() {
		testLeveling(false);
	}
	
	@Test
	public void testPreserveHigherLevelWhenAddingLowerLevel() {
		testLeveling(true);
	}
	
	private void testLeveling(boolean heaviestFirst) {
		// Create 3 inner-caches with leve-intervals 0-1, 2-4 and 5-infinite
		final int[] LEVEL_SPLIT_AFTER = new int[]{1, 4, Integer.MAX_VALUE};
		
		final List<Cache<String, Map<String, String>>> LEVEL_CACHES = new ArrayList<Cache<String, Map<String, String>>>(LEVEL_SPLIT_AFTER.length); 
		
		final ChangingValueAndLevelMultiCache.Builder<String, Map<String, String>> underTestBuilder = createCacheBuilder(new EmptyMapNewCreator(), null);
		// Level for a cache-entry will be the size of its map-value
		underTestBuilder.levelCalculator(new MapSizeLevelCalculator());
		
		int currentLevelIntervalStart = 0;
		for (int currentLevelIntervalEnd : LEVEL_SPLIT_AFTER) {
			Cache<String, Map<String, String>> innerCache = CacheBuilder.newBuilder().maximumSize(MAX_CACHE_SIZE).build();
			LEVEL_CACHES.add(innerCache);
			underTestBuilder.addCache(innerCache, currentLevelIntervalStart, currentLevelIntervalEnd, "Inner Cache " + currentLevelIntervalStart + "-" + currentLevelIntervalEnd + " size");
			currentLevelIntervalStart = currentLevelIntervalEnd+1;
		}
		
		final ChangingValueAndLevelMultiCache<String, Map<String, String>> underTest = underTestBuilder.build();
		
		// Generate entries for the cache and add them to the cache
		// * 2*max-cache-size entries for each inner-cache
		// * Each inner-cache will have its entries spread over its level-interval (as much as possible)
		currentLevelIntervalStart = 0;
		int nextPrefixIndex = 1;
		// Remember for asserting the full amount data added to the cache
		Map<String, Map<String, String>> addedToCache = new HashMap<String, Map<String, String>>();
		for (int currentLevelIntervalEnd : LEVEL_SPLIT_AFTER) {
			for (int i = 0; i < 2*MAX_CACHE_SIZE; i++) {
				int mapEntriesForPrefix = (i%(currentLevelIntervalEnd-currentLevelIntervalStart+1))+currentLevelIntervalStart;
				if (mapEntriesForPrefix == 0) mapEntriesForPrefix = 1;
				String prefix = "prefix" + nextPrefixIndex;
				addedToCache.put(prefix, new HashMap<String, String>());
				for (int suffixCounter = 1; suffixCounter <= mapEntriesForPrefix; suffixCounter++) {
					String suffix = "suffix" + suffixCounter;
					String value = prefix + "_" + suffix + "_value";
					addedToCache.get(prefix).put(suffix, value);
				}
				nextPrefixIndex++;
			}
			currentLevelIntervalStart = currentLevelIntervalEnd+1;
		}
		
		for (int i = 1; i < nextPrefixIndex; i++) {
			int index = (heaviestFirst)?(nextPrefixIndex-i):i;
			String prefix = "prefix" + index;
			Map<String, String> value = addedToCache.get(prefix);
			for (Map.Entry<String, String> valueEntry : value.entrySet()) {
				underTest.modify(prefix, new AddKeyValueModifier(valueEntry.getKey(), valueEntry.getValue()), true);
			}
		}
		
		// ASSERT
		
		Collection<Cache<String, Map<String, String>>> underTestCaches = underTest.getAllCaches();
		// Expect a level-caches plus default-cache to in the cache-collection of cache under test
		Assert.assertEquals(LEVEL_CACHES.size() + 1, underTestCaches.size());
		for (Cache<String, Map<String, String>> levelCache : LEVEL_CACHES) {
			Assert.assertTrue(underTestCaches.contains(levelCache));
		}
		
		currentLevelIntervalStart = 0;
		nextPrefixIndex = 1;
		int currentInterval = 0;
		int[] intervalCacheEntries = new int[LEVEL_SPLIT_AFTER.length];
		for (int currentLevelIntervalEnd : LEVEL_SPLIT_AFTER) {
			for (int i = 0; i < 2*MAX_CACHE_SIZE; i++) {
				int mapEntriesForPrefix = (i%(currentLevelIntervalEnd-currentLevelIntervalStart+1))+currentLevelIntervalStart;
				if (mapEntriesForPrefix == 0) mapEntriesForPrefix = 1;
				String prefix = "prefix" + nextPrefixIndex;
				Map<String, String> value = underTest.getIfPresent(prefix);
				if (value != null) {
					// Remember how many entries we saw in the cache for this particular level
					intervalCacheEntries[currentInterval]++;
					// If an prefix is in cache it has to be complete (contain all the suffixes in the map-value)
					Assert.assertEquals(currentLevelIntervalStart + "-" + currentLevelIntervalEnd, addedToCache.get(prefix), value);
				}
				nextPrefixIndex++;
			}
			currentLevelIntervalStart = currentLevelIntervalEnd+1;
			currentInterval++;
		}
		
		currentLevelIntervalStart = 0;
		currentInterval = 0;
		for (int currentLevelIntervalEnd : LEVEL_SPLIT_AFTER) {
			// For each level we should see MAX_CACHE_SIZE entries
			// In case of !HEAVIEST_FIRST: MAX_CACHE_SIZE-1 because one slot has been used by the "heavier" entries before
			// they became heavy enough to leave this level-interval. Hence not -1 for the last level-interval
			Assert.assertEquals(currentLevelIntervalStart + "-" + currentLevelIntervalEnd, MAX_CACHE_SIZE - (((currentInterval+1) == intervalCacheEntries.length)?0:((heaviestFirst)?0:1)), intervalCacheEntries[currentInterval]);
			currentLevelIntervalStart = currentLevelIntervalEnd+1;
			currentInterval++;
		}
	}
	
	private class EmptyMapNewCreator implements Supplier<Map<String, String>> {

		@Override
		public Map<String, String> get() {
			return new HashMap<String, String>();
		}
		
	}
	
	private class AddKeyValueModifier implements Function<Map<String, String>, Map<String, String>> {
		
		private String addKey;
		private String addValue;
		
		private AddKeyValueModifier(String addKey, String addValue) {
			this.addKey = addKey;
			this.addValue = addValue;
		}

		@Override
		public Map<String, String> apply(Map<String, String> value) {
			value.put(addKey, addValue);
			return value;
		}
		
	}
	
	private class MapSizeLevelCalculator implements ChangingValueAndLevelMultiCache.BiFunction<String, Map<String, String>, Integer> {

		@Override
		public Integer apply(String key, Map<String, String> value) {
			return value.size();
		}

		
	}
	
}
