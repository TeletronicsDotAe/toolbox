package ae.teletronics.cache;

/**
 * Just a simple carrier of two objects - for methods parameters and return-values etc.
 *
 * @param <T1> Type of first in pair
 * @param <T2> Type of second in pair
 */
public class Pair<T1, T2> {

	public final T1 _1;
	public final T2 _2;
	
	public Pair(T1 v1, T2 v2) {
		_1 = v1;
		_2 = v2;
	}
}
