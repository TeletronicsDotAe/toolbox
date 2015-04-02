package ae.teletronics.cache;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

public class ChangingValueAndLevelMultiCacheTest extends ChangingValueCaheTest {

	@Override
	protected <V> ChangingValueCache<String, V> createCache(Function<V, V> addOneModifier, Supplier<V> zeroNewCreator) {
		Cache<String, V> defaultInnerCache = CacheBuilder.newBuilder().maximumSize(MAX_CACHE_SIZE).build();
		ChangingValueAndLevelMultiCache.Builder<String, V> underTestBuilder = ChangingValueAndLevelMultiCache.builder();
		return underTestBuilder
				.cache(defaultInnerCache)
				.defaultModifier(addOneModifier)
				.defaultNewCreator(zeroNewCreator)
				.levelCalculator(new ChangingValueAndLevelMultiCache.BiFunction<String, V, Integer>() {

					@Override
					public Integer apply(String t, V u) {
						return 1;
					}
					
				})
				.build();
	}
	
}
