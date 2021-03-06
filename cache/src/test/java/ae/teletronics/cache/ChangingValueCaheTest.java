package ae.teletronics.cache;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

public class ChangingValueCaheTest {

	protected static final int MAX_CACHE_SIZE = 10;
	
	private ChangingValueCache<String, Integer> underTest;
	
	@Before
	public void before() {
		underTest = createCacheBuilder(new ZeroNewCreator(), new AddOneModifier()).build();
	}
	
	protected <V> ChangingValueCache.Builder<String, V> createCacheBuilder(Supplier<V> defaultNewCreator, Function<V, V> defaultModifier) {
		Cache<String, V> innerCache = CacheBuilder.newBuilder().maximumSize(MAX_CACHE_SIZE).build();
		ChangingValueCache.Builder<String, V> underTestBuilder = ChangingValueCache.builder();
		return underTestBuilder
				.cache(innerCache)
				.defaultNewCreator(defaultNewCreator)
				.defaultModifier(defaultModifier);
	}
	
	@Test
	public void testGetAddIfNotPresent() {
		assertEquals(0, underTest.getAddIfNotPresent("Not present default creator").intValue());
		assertEquals(1, underTest.getAddIfNotPresent("Not present provided creator", new Supplier<Integer>() {

			@Override
			public Integer get() {
				return 1;
			}
			
		}).intValue());
	}

	@Test
	public void getIfPresent() {
		assertNull(underTest.getIfPresent("Not present"));
		Integer addedValue = underTest.getAddIfNotPresent("Now present");
		assertNotNull(addedValue);
		assertEquals(addedValue, underTest.getIfPresent("Now present"));
	}

	@Test
	public void testSize() {
		assertEquals(0, underTest.size());
		
		for (int i = 1; i <= MAX_CACHE_SIZE; i++) {
			underTest.getAddIfNotPresent("key" + i);
			assertEquals(i, underTest.size());
		}
		
		for (int i = MAX_CACHE_SIZE+1; i <= MAX_CACHE_SIZE*2; i++) {
			underTest.getAddIfNotPresent("key" + i);
			assertEquals(MAX_CACHE_SIZE, underTest.size());
		}
	}
	
	@Test
	public void testModify() {
		assertNull(underTest.modify("Not present", false));
		assertEquals(1, underTest.modify("Now present", true).intValue());
		
		assertNull(underTest.modify("Not present", new NoModificationModifier(), false));
		assertEquals(0, underTest.modify("Now present 2", new NoModificationModifier(), true).intValue());

		assertNull(underTest.modify("Not present", new ZeroNewCreator(), false));
		assertNull(underTest.modify("Not present", new NullNewCreator(), true));
		assertEquals(2, underTest.modify("Now present 3", new OneNewCreator(), true).intValue());
		
		assertNull(underTest.modify("Not present", new ZeroNewCreator(), new NoModificationModifier(), false));
		assertNull(underTest.modify("Not present", new NullNewCreator(), new NoModificationModifier(), true));
		assertEquals(1, underTest.modify("Now present 4", new OneNewCreator(), new NoModificationModifier(), true).intValue());
		assertEquals(1, underTest.modify("Now present 5", new ZeroNewCreator(), new AddOneModifier(), true).intValue());
		assertEquals(2, underTest.modify("Now present 6", new OneNewCreator(), new AddOneModifier(), true).intValue());
	}

	@Test
	public void testModifySupportRecursiveCalls() {
		final ChangingValueCache<String, IntegerCarrier> underTest = createCacheBuilder(new ZeroIntegerCarrierNewCreator(), new AddOneIntegerCarrierModifier()).build();
		assertEquals(6,
			underTest.modify("Now present", new FiveIntegerCarrierNewCreator(),
					new Function<IntegerCarrier, IntegerCarrier>() {
			
						@Override
						public IntegerCarrier apply(IntegerCarrier value) {
							return underTest.modify("Now present", new AddOneIntegerCarrierModifier(), false);
						}
						
					}, true, true).val.intValue()
		);
	}

	@Test
	public void testModifySupportRecursiveCallsReplacingValue() {
		final ChangingValueCache<String, IntegerCarrier> underTest = createCacheBuilder(new ZeroIntegerCarrierNewCreator(), new AddOneIntegerCarrierModifier()).build();
		try {
			underTest.modify("Now present", new OneIntegerCarrierNewCreator(),
					new Function<IntegerCarrier, IntegerCarrier>() {
			
						@Override
						public IntegerCarrier apply(IntegerCarrier value) {
							return underTest.modify("Now present", new Function<IntegerCarrier, IntegerCarrier>() {
								@Override
								public IntegerCarrier apply(IntegerCarrier value) {
									IntegerCarrier result = new IntegerCarrier();
									result.val = 11;
									return result;
								}
							}, false);
						}
						
					}, true, true);
			fail();
		} catch (Exception e) {
			assertTrue(e instanceof RuntimeException);
			assertEquals("Modifier called modify with a modifier that replaced value object with another value object", e.getMessage());
		}
	}
	
	@Test
	public void testModifyAll() {
		underTest.getAddIfNotPresent("Modify 2", new ConstantNewCreator(2));
		underTest.getAddIfNotPresent("Modify 3", new ConstantNewCreator(3));
		underTest.getAddIfNotPresent("Modify 4", new ConstantNewCreator(4));
		underTest.getAddIfNotPresent("Modify 5", new ConstantNewCreator(5));
		underTest.getAddIfNotPresent("Dont modify 2", new ConstantNewCreator(2));
		underTest.getAddIfNotPresent("Dont modify 3", new ConstantNewCreator(3));
		underTest.getAddIfNotPresent("Dont modify 4", new ConstantNewCreator(4));
		underTest.getAddIfNotPresent("Dont modify 5", new ConstantNewCreator(5));
		
		// Modify (add one) to all entries having a key starting with "Modify" and value 3 or 4
		underTest.modifyAll(new Predicate<String>() {

			@Override
			public boolean apply(String input) {
				return input.startsWith("Modify");
			}
			
		}, new Predicate<Integer>() {

			@Override
			public boolean apply(Integer input) {
				return 2 < input && input < 5;
			}
			
		});
		
		// None should have their value changes except...
		assertEquals(2, underTest.getIfPresent("Modify 2").intValue());
		// ...this...
		assertEquals(4, underTest.getIfPresent("Modify 3").intValue());
		// ...and this
		assertEquals(5, underTest.getIfPresent("Modify 4").intValue());
		assertEquals(5, underTest.getIfPresent("Modify 5").intValue());
		assertEquals(2, underTest.getIfPresent("Dont modify 2").intValue());
		assertEquals(3, underTest.getIfPresent("Dont modify 3").intValue());
		assertEquals(4, underTest.getIfPresent("Dont modify 4").intValue());
		assertEquals(5, underTest.getIfPresent("Dont modify 5").intValue());
	}
	
	@Test
	public void testConcurrency() {
		Runnable task = new Runnable() {

			@Override
			public void run() {
				underTest.modify("key", true);
			}
			
		};
		
		final int NO_THREADS = 10;
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
		
		assertEquals(NO_THREADS, underTest.getIfPresent("key").intValue());
	}
	
	private class ConstantNewCreator implements Supplier<Integer> {
		
		private Integer newValue;
		
		private ConstantNewCreator(Integer newValue) {
			this.newValue = newValue;
		}

		@Override
		public Integer get() {
			return newValue;
		}
		
		
	}
	
	private class NullNewCreator extends ConstantNewCreator {
		private NullNewCreator() { super(null); }
	}

	
	private class ZeroNewCreator extends ConstantNewCreator {
		private ZeroNewCreator() { super(0); }
	}

	private class OneNewCreator extends ConstantNewCreator {
		private OneNewCreator() { super(1); }
	}

	private class NoModificationModifier implements Function<Integer, Integer> {

		@Override
		public Integer apply(Integer value) {
			return value;
		}
		
	}
	
	private class AddOneModifier implements Function<Integer, Integer> {

		@Override
		public Integer apply(Integer value) {
			return value + 1;
		}
		
	}
	
	private class IntegerCarrier {
		private Integer val;
	}
	
	private class AddOneIntegerCarrierModifier implements Function<IntegerCarrier, IntegerCarrier> {

		@Override
		public IntegerCarrier apply(IntegerCarrier value) {
			value.val += 1;
			return value;
		}
		
	}

	private class ConstantIntegerCarrierNewCreator implements Supplier<IntegerCarrier> {

		private Integer newValue;
		
		private ConstantIntegerCarrierNewCreator(Integer newValue) {
			this.newValue = newValue;
		}
		
		@Override
		public IntegerCarrier get() {
			IntegerCarrier result = new IntegerCarrier();
			result.val = newValue;
			return result;
		}
		
	}

	private class ZeroIntegerCarrierNewCreator extends ConstantIntegerCarrierNewCreator {
		private ZeroIntegerCarrierNewCreator() { super(0); }
	}
	
	private class OneIntegerCarrierNewCreator extends ConstantIntegerCarrierNewCreator {
		private OneIntegerCarrierNewCreator() { super(0); }
	}

	private class FiveIntegerCarrierNewCreator extends ConstantIntegerCarrierNewCreator {
		private FiveIntegerCarrierNewCreator() { super(5); }
	}

}
