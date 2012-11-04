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
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.infra.utils.GenericFactory;
import org.infra.utils.PrimeFinder;

/**
 * Native Integer HashMap
 * This class is NOT Thread-Safe
 *
 * @param <V> type of values
 *
 * @author Guillermo Grandes / guillermo.grandes[at]gmail.com
 */
public class IntHashMap<V> implements Iterable<V> {

	private int elementCount;
	private IntEntry<V>[] elementData;

	private final float loadFactor;
	private int threshold;
	private int defaultSize = 17;

	private GenericFactory<V> factory;

	/**
	 * Constructs a new {@code IntHashMap} instance with the specified capacity.
	 *
	 * @param capacity the initial capacity of this hash map.
	 * @param type class for values
	 */
	public IntHashMap(final int capacity, final Class<V> type) {
		factory = new GenericFactory<V>(type);
		defaultSize = primeSize(capacity);
		if (capacity >= 0) {
			elementCount = 0;
			elementData = newElementArray(capacity == 0 ? 1 : capacity);
			loadFactor = 0.75f; // Default load factor of 0.75
			initCache(elementData.length);
			computeMaxSize();
		} else {
			throw new IllegalArgumentException();
		}
	}

	/**
	 * Constructs a new {@code IntHashMap} instance with default capacity (17).
	 *
	 * @param type class for values
	 */
	public IntHashMap(final Class<V> type) {
		this(17, type);
	}

	@SuppressWarnings("unchecked")
	private IntEntry<V>[] newElementArray(int s) {
		return new IntEntry[s];
	}


	/**
	 * Removes all mappings from this hash map, leaving it empty.
	 *
	 * @see #isEmpty
	 * @see #size
	 */
	public void clear() {
		clear(true);
	}

	/**
	 * Clear the map
	 * @param shrink if true shrink the map to initial size
	 */
	@SuppressWarnings("unchecked")
	public void clear(final boolean shrink) {
		clearCache();
		if (elementCount > 0) {            
			elementCount = 0;            
		}
		if (shrink && (elementData.length > 1024) && (elementData.length > defaultSize)) {
			elementData = new IntEntry[defaultSize];
		}
		else {
			Arrays.fill(elementData, null);
		}
		computeMaxSize();
	}

	private void computeMaxSize() {
		threshold = (int) (elementData.length * loadFactor);
	}


	/**
	 * Returns the value of specified key.
	 *
	 * @param key the key.
	 * @return the value of the mapping with the specified key, or {@code null}
	 *         if no mapping for the specified key is found.
	 */
	public V get(final int key) {
		final int index = (key & 0x7FFFFFFF) % elementData.length;

		IntEntry<V> m = elementData[index];
		while (m != null) {
			if (key == m.key)
				return m.value;
			m = m.nextInSlot;
		}
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
	 *         {@code null} if there was no such mapping.
	 */
	public V put(final int key, final V value) {
		int index = (key & 0x7FFFFFFF) % elementData.length;

		IntEntry<V> entry = elementData[index];
		while (entry != null && key != entry.key) {
			entry = entry.nextInSlot;
		}

		if (entry == null) {
			if (++elementCount > threshold) {
				rehash();
				index = (key & 0x7FFFFFFF) % elementData.length;
			}
			entry = createHashedEntry(key, index);
		}

		V result = entry.value;
		entry.value = value;
		return result;
	}


	IntEntry<V> createHashedEntry(final int key, final int index) {
		IntEntry<V> entry = reuseAfterDelete();
		if (entry == null) {
			entry = new IntEntry<V>(key);
		} 
		else {
			entry.key = key;
			entry.value = null;
		}

		entry.nextInSlot = elementData[index];
		elementData[index] = entry;
		return entry;
	}

	void rehash(final int capacity) {
		final int length = primeSize(capacity == 0 ? 1 : capacity << 1);
		//System.out.println(this.getClass().getName() + "::rehash() old=" + elementData.length + " new=" + length);

		IntEntry<V>[] newData = newElementArray(length);
		for (int i = 0; i < elementData.length; i++) {
			IntEntry<V> entry = elementData[i];
			while (entry != null) {
				int index = (entry.key & 0x7FFFFFFF) % length;
				IntEntry<V> next = entry.nextInSlot;
				entry.nextInSlot = newData[index];
				newData[index] = entry;
				entry = next;
			}
		}
		elementData = newData;
		computeMaxSize();
	}

	void rehash() {
		rehash(elementData.length);
	}

	/**
	 * Removes the mapping with the specified key from this map.
	 *
	 * @param key the key of the mapping to remove.
	 * @return the value of the removed mapping or {@code null} if no mapping
	 *         for the specified key was found.
	 */
	public V remove(final int key) {
		IntEntry<V> entry = removeEntry(key);
		if (entry == null)
			return null;
		V ret = entry.value;
		reuseAfterDelete(entry);

		return ret;
	}

	IntEntry<V> removeEntry(final int key) {
		IntEntry<V> last = null;

		final int index = (key & 0x7FFFFFFF) % elementData.length;
		IntEntry<V> entry = elementData[index];

		while (true) {
			if (entry == null)
				return null;

			if (key == entry.key) {
				if (last == null) {
					elementData[index] = entry.nextInSlot;
				} else {
					last.nextInSlot = entry.nextInSlot;
				}
				elementCount--;
				return entry;
			}

			last = entry;
			entry = entry.nextInSlot;
		}
	}

	/**
	 * Returns the number of elements in this map.
	 *
	 * @return the number of elements in this map.
	 */
	public int size() {
		return elementCount;
	}

	// ========== Entry Cache

	/*
	private transient Entry<V> reuseAfterDelete = null;

	private void initCache(int size) {}
	private void clearCache() {}

	private Entry<V> reuseAfterDelete() {
		final Entry<V> ret = reuseAfterDelete;
		reuseAfterDelete = null;
		return ret;
	}
	private void reuseAfterDelete(final Entry<V> entry) {
		entry.clean();
		reuseAfterDelete = entry;
	}
	 */

	private ArrayDeque<IntEntry<V>> cache;

	private void initCache(final int size) {
		cache = new ArrayDeque<IntEntry<V>>(size);
	}
	public void clearCache() { 
		cache.clear();
	}
	private IntEntry<V> reuseAfterDelete() {
		return cache.pollLast();
	}
	private void reuseAfterDelete(final IntEntry<V> entry) {
		entry.clean();
		cache.offerLast(entry);
	}

	// ========== Internal Entry

	static final class IntEntry<V> {
		IntEntry<V> nextInSlot;
		int key;
		V value;

		IntEntry(int theKey) {
			this.key = theKey;
			this.value = null;
		}
		void clean() {
			value = null;
			key = Integer.MIN_VALUE;
			nextInSlot = null;
		}
	}

	// ========== Prime Finder

	private static final int primeSize(final int capacity) {
		//return java.math.BigInteger.valueOf((long)capacity).nextProbablePrime().intValue();
		return PrimeFinder.nextPrime(capacity);
	}

	// ========== Iterator

	/**
	 * @returns iterator over values in map
	 */
	public Iterator<V> iterator() {
		return new IntHashMapIterator<V>(this);
	}

	static class IntHashMapIterator<V> implements Iterator<V> {
		private int position = 0;

		boolean canRemove = false;
		IntEntry<V> entry;
		IntEntry<V> lastEntry;
		final IntHashMap<V> associatedMap;

		IntHashMapIterator(IntHashMap<V> hm) {
			associatedMap = hm;
		}

		public boolean hasNext() {
			if (entry != null) {
				return true;
			}

			IntEntry<V>[] elementData = associatedMap.elementData;
			int length = elementData.length;
			int newPosition = position;
			boolean result = false;

			while (newPosition < length) {
				if (elementData[newPosition] == null) {
					newPosition++;
				} 
				else {
					result = true;
					break;
				}
			}

			position = newPosition;
			return result;
		}

		public V next() {
			if (!hasNext()) {
				throw new NoSuchElementException();
			}

			IntEntry<V> result;
			IntEntry<V> _entry = entry;
			if (_entry == null) {
				result = lastEntry = associatedMap.elementData[position++];
				entry = lastEntry.nextInSlot;
			} 
			else {
				if (lastEntry.nextInSlot != _entry) {
					lastEntry = lastEntry.nextInSlot;
				}
				result = _entry;
				entry = _entry.nextInSlot;
			}
			canRemove = true;
			return result.value;
		}

		public void remove() {
			if (!canRemove) {
				throw new IllegalStateException();
			}

			canRemove = false;

			if (lastEntry.nextInSlot == entry) {
				while (associatedMap.elementData[--position] == null) {
					// Skip
				}
				associatedMap.elementData[position] = associatedMap.elementData[position].nextInSlot;
				entry = null;
			} 
			else {
				lastEntry.nextInSlot = entry;
			}
			if (lastEntry != null) {
				IntEntry<V> reuse = lastEntry;
				lastEntry = null;
				associatedMap.reuseAfterDelete(reuse);
			}

			associatedMap.elementCount--;
		}
	}

	// =========================================

	/**
	 * Return an array with values in this map
	 * @return array with values
	 */
	public V[] getValues() {
		final V[] array = factory.newArray(elementCount);
		int i = 0;
		for (final V v : this) {
			array[i++] = v;
		}
		return array;
	}

	// =========================================

	@SuppressWarnings("unused")
	public static void main(String[] args) {
		if (false) {
			long capacity = 1;
			int count = 1;
			while (capacity < Integer.MAX_VALUE) {
				capacity = java.math.BigInteger.valueOf(capacity).nextProbablePrime().longValue();
				System.out.print(capacity + ", ");
				final double inc = Math.log(2)/Math.log(capacity<<5) * 10 + 1; 
				//System.out.println(inc);
				capacity *= inc; 
				if (count % 5 == 0) System.out.println();
				count++;
			}
			System.out.println(Integer.MAX_VALUE);
			System.out.println("------");

			System.out.println(count);
			System.out.println(PrimeFinder.nextPrime((int)1e6));
		}
		if (true) {
			IntHashMap<Integer> hash = new IntHashMap<Integer>(16, Integer.class);
			hash.put(1, 2);
			hash.put(2, 4);
			for (Integer i : hash.getValues()) {
				System.out.println(i);
			}
		}
	}
}
