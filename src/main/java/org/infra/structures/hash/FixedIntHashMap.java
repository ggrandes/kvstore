/*
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package org.infra.structures.hash;
import java.util.Arrays;
import java.util.Random;

import org.infra.utils.GenericFactory;
import org.infra.utils.PrimeFinder;

/**
 * Native Integer HashMap with Fixed Size (no collision resolver); 
 * on collision last key/value overwrite old key/value
 * Suitable only for caches
 * 
 * This class is NOT Thread-Safe
 *
 * @author Guillermo Grandes / guillermo.grandes[at]gmail.com
 */
public class FixedIntHashMap<T> {
	public static final boolean DEBUG = false;

	private int elementCount;

	private int[] elementKeys;
	private T[] elementValues;

	private final float loadFactor;
	private final GenericFactory<T> arrayFactory;

	private int threshold;

	private int defaultSize = 16;
	private int collisions = 0;

	private int[] newKeyArray(final int size) {
		if (DEBUG) System.out.println(this.getClass().getName() + "::newKeyArray("+size+")");
		final int[] e = new int[size];
		Arrays.fill(e, Integer.MIN_VALUE);
		return e;
	}
	private T[] newElementArray(final int size) {
		if (DEBUG) System.out.println(this.getClass().getName() + "::newElementArray("+size+")");
		final T[] e = arrayFactory.newArray(size);
		Arrays.fill(e, null);
		return e;
	}

	/**
	 * Constructs a new {@code NativeFixedIntHashMap} instance with the specified capacity.
	 *
	 * @param capacity the initial capacity of this hash map.
	 * @throws IllegalArgumentException when the capacity is less than zero.
	 */
	public FixedIntHashMap(final int capacity, final Class<T> ctype) {
		arrayFactory = new GenericFactory<T>(ctype);
		defaultSize = primeSize(capacity);
		if (capacity > 0) {
			elementCount = 0;
			elementKeys = newKeyArray(defaultSize);
			elementValues = newElementArray(defaultSize);
			loadFactor = 0.75f; // Default load factor of 0.75
			computeMaxSize();
		} else {
			throw new IllegalArgumentException();
		}
	}

	private static final int primeSize(final int capacity) {
		//return java.math.BigInteger.valueOf((long)capacity).nextProbablePrime().intValue();
		return PrimeFinder.nextPrime(capacity);
	}

	/**
	 * Removes all mappings from this hash map, leaving it empty.
	 *
	 * @see #isEmpty
	 * @see #size
	 */
	public void clear(boolean shrink) {
		if (elementCount > 0) {            
			elementCount = 0;            
		}
		if (shrink && ((elementKeys.length > 1024) && (elementKeys.length > defaultSize))) {
			elementKeys = newKeyArray(defaultSize);
			elementValues = newElementArray(defaultSize);
		}
		else {
			Arrays.fill(elementKeys, Integer.MIN_VALUE);
			Arrays.fill(elementValues, null);
		}
		computeMaxSize();
	}

	public int[] getKeys() {
		return elementKeys;
	}
	public T[] getValues() {
		return elementValues;
	}

	/**
	 * Returns a shallow copy of this map.
	 *
	 * @return a shallow copy of this map.
	 */
	private void computeMaxSize() {
		threshold = (int) (elementKeys.length * loadFactor);
		if (DEBUG) System.out.println(this.getClass().getName() + "::computeMaxSize()="+threshold + " collisions=" + collisions + " (" + (collisions * 100 / elementKeys.length) + ")");
		collisions = 0;
	}

	/**
	 * Returns the value of the mapping with the specified key.
	 *
	 * @param key the key.
	 * @return the value of the mapping with the specified key, or {@code -1}
	 *         if no mapping for the specified key is found.
	 */
	public T get(final int key) {
		int index = ((key & 0x7FFFFFFF) % elementKeys.length);

		long m = elementKeys[index];
		if (key == m)
			return elementValues[index];

		return null;
	}


	/**
	 * Returns whether this map is empty.
	 *
	 * @return {@code true} if this map has no elements, {@code false}
	 *         otherwise.
	 * @see #size()
	 */
	public boolean isEmpty() {
		return (elementCount == 0);
	}

	/**
	 * Maps the specified key to the specified value.
	 *
	 * @param key   the key.
	 * @param value the value.
	 * @return the value of any previous mapping with the specified key or
	 *         {@code -1} if there was no such mapping.
	 */
	public T put(final int key, final T value) {
		int index = ((key & 0x7FFFFFFF) % elementKeys.length);
		T oldvalue = null;

		long entry = elementKeys[index];
		if (entry == Integer.MIN_VALUE) {
			++elementCount;
		}
		else {
			oldvalue = elementValues[index];
			collisions++;
		}
		elementKeys[index] = key;
		elementValues[index] = value;
		return oldvalue;
	}

	/**
	 * Removes the mapping with the specified key from this map.
	 *
	 * @param key the key of the mapping to remove.
	 * @return the value of the removed mapping or {@code null} if no mapping
	 *         for the specified key was found.
	 */

	public T remove(final int key) {
		int index = ((key & 0x7FFFFFFF) % elementKeys.length);

		int entry = elementKeys[index];
		if (key == entry) {
			final T oldvalue = elementValues[index];
			elementKeys[index] = Integer.MIN_VALUE;
			elementValues[index] = null;
			elementCount--;
			return oldvalue;
		}
		return null;
	}

	/**
	 * Returns the number of elements in this map.
	 *
	 * @return the number of elements in this map.
	 */

	public int size() {
		return elementCount;
	}

	public static void main(String[] args) throws Exception {
		FixedIntHashMap<Long> f = new FixedIntHashMap<Long>(1000000, Long.class);
		long ts, ts2;
		Random r = new Random();
		ts = System.currentTimeMillis(); ts2 = ts;
		for (int i = 1; i < 1e3; i++) {
			f.put(i, (long)(i));
		}
		for (int i = 1; i < 1e3; i++) {
			System.out.println(f.get(i));
		}
		for (int i = 1; i < 1; i++) {
			f.put((int)(r.nextLong() & 0x7FFFFFFFL), r.nextLong() & 0x7FFFFFFFFFFFFFFFL);
			if (i % 10000 == 0) {
				System.out.println(i + "\t" + (System.currentTimeMillis() - ts2));
				ts2 = System.currentTimeMillis();
			}
		}
		System.out.println("INSERT: " + (System.currentTimeMillis() - ts));
	}

}

