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
package org.kvstore.structures.hash;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.Arrays;

import org.kvstore.utils.PrimeFinder;

/**
 * A hashtable-based Set implementation with weak values. An entry in a
 * WeakSet will automatically be removed when its value is no longer in
 * ordinary use. More precisely, the presence of a given value will
 * not prevent the value from being discarded by the garbage collector, that is,
 * made finalizable, finalized, and then reclaimed. When a value has been
 * discarded its entry is effectively removed from the set, so this class
 * behaves somewhat differently from other Set implementations.
 * <p>
 * This class is NOT Thread-Safe
 * 
 * @see java.util.WeakHashMap
 * 
 * @author Guillermo Grandes / guillermo.grandes[at]gmail.com
 */
public class WeakSet<T> {

	private int elementCount;
	private Entry<T>[] elementData;

	private final float loadFactor;
	private int threshold;
	private int defaultSize = 17;

	/**
	 * Reference queue for cleared WeakEntry
	 */
	private final ReferenceQueue<T> queue = new ReferenceQueue<T>();

	/**
	 * Constructs a new {@code WeakSet} instance with the specified capacity.
	 * 
	 * @param capacity the initial capacity of this set.
	 * @param type class for values
	 */
	public WeakSet(final int capacity) {
		defaultSize = primeSize(capacity);
		if (capacity >= 0) {
			elementCount = 0;
			elementData = newElementArray(capacity < 0 ? 1 : capacity);
			loadFactor = 0.75f; // Default load factor of 0.75
			computeMaxSize();
		} else {
			throw new IllegalArgumentException();
		}
	}

	/**
	 * Constructs a new {@code WeakSet} instance with default capacity (17).
	 */
	public WeakSet() {
		this(17);
	}

	/**
	 * Check for equal objects
	 * 
	 * @param o1
	 * @param o2
	 * @return true if equals
	 */
	private final static boolean eq(Object o1, Object o2) {
		return ((o1 == o2) || o1.equals(o2));
	}

	/**
	 * Removes all values from this WeakSet, leaving it empty.
	 * 
	 * @see #isEmpty
	 * @see #size
	 */
	public final void clear() {
		clear(true);
	}

	/**
	 * Clear the set
	 * 
	 * @param shrink if true, shrink the set to initial size
	 */
	public void clear(final boolean shrink) {
		while (queue.poll() != null);
		if (elementCount > 0) {
			elementCount = 0;
		}
		if (shrink && (elementData.length > 1024) && (elementData.length > defaultSize)) {
			elementData = newElementArray(defaultSize);
		} else {
			Arrays.fill(elementData, null);
		}
		computeMaxSize();
		while (queue.poll() != null);
	}

	/**
	 * Returns the specified value.
	 * 
	 * @param value the value.
	 * @return the value, or {@code null} if not found the specified value
	 */
	public T get(final T value) {
		expungeStaleEntries();
		//
		final int index = (value.hashCode() & 0x7FFFFFFF) % elementData.length;
		Entry<T> m = elementData[index];
		while (m != null) {
			if (eq(value, m.get()))
				return m.get();
			m = m.nextInSlot;
		}
		return null;
	}

	/**
	 * Returns whether this set is empty.
	 * 
	 * @return {@code true} if this set has no elements, {@code false} otherwise.
	 * @see #size()
	 */
	public final boolean isEmpty() {
		return (size() == 0);
	}

	/**
	 * Puts the specified value in the set.
	 * 
	 * @param value
	 *            the value.
	 * @return the value of any previous put or {@code null} if there was no such value.
	 */
	public T put(final T value) {
		expungeStaleEntries();
		//
		final int hash = value.hashCode();
		int index = (hash & 0x7FFFFFFF) % elementData.length;
		Entry<T> entry = elementData[index];
		while (entry != null && !eq(value, entry.get())) {
			entry = entry.nextInSlot;
		}

		if (entry == null) {
			if (++elementCount > threshold) {
				expandElementArray(elementData.length);
				index = (hash & 0x7FFFFFFF) % elementData.length;
			}
			entry = createHashedEntry(value, index);
			return null;
		}

		final T result = entry.get();
		return result;
	}

	private final Entry<T> createHashedEntry(final T valye, final int index) {
		Entry<T> entry = new Entry<T>(valye, queue);
		entry.nextInSlot = elementData[index];
		elementData[index] = entry;
		return entry;
	}

	private final void computeMaxSize() {
		threshold = (int) (elementData.length * loadFactor);
	}

	@SuppressWarnings("unchecked")
	private final Entry<T>[] newElementArray(int s) {
		return new Entry[s];
	}

	private final void expandElementArray(final int capacity) {
		final int length = primeSize(capacity < 0 ? 1 : capacity << 1);
		final Entry<T>[] newData = newElementArray(length);
		for (int i = 0; i < elementData.length; i++) {
			Entry<T> entry = elementData[i];
			elementData[i] = null;
			while (entry != null) {
				final Entry<T> next = entry.nextInSlot;
				final T value = entry.get();
				if (value == null) {
					entry.nextInSlot = null;
					elementCount--;
				} else {
					final int index = (entry.hash & 0x7FFFFFFF) % length;
					entry.nextInSlot = newData[index];
					newData[index] = entry;
				}
				entry = next;
			}
		}
		elementData = newData;
		computeMaxSize();
	}

	@SuppressWarnings("unchecked")
	private final void expungeStaleEntries() {
		Entry<T> entry;
		while ((entry = (Entry<T>) queue.poll()) != null) {
			final int i = (entry.hash & 0x7FFFFFFF) % elementData.length;

			Entry<T> prev = elementData[i];
			Entry<T> p = prev;
			while (p != null) {
				Entry<T> next = p.nextInSlot;
				if (p == entry) {
					if (prev == entry) {
						elementData[i] = next;
					} else {
						prev.nextInSlot = next;
					}
					entry.nextInSlot = null;
					elementCount--;
					break;
				}
				prev = p;
				p = next;
			}
		}
	}

	/**
	 * Removes the specified value from this set.
	 * 
	 * @param value the value to remove.
	 * @return the value removed or {@code null} if not found
	 */
	public T remove(final T value) {
		expungeStaleEntries();
		//
		final Entry<T> entry = removeEntry(value);
		if (entry == null)
			return null;
		final T ret = entry.get();
		return ret;
	}

	private final Entry<T> removeEntry(final T value) {
		Entry<T> last = null;

		final int index = (value.hashCode() & 0x7FFFFFFF) % elementData.length;
		Entry<T> entry = elementData[index];

		while (true) {
			if (entry == null)
				return null;

			if (eq(value, entry.get())) {
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
	 * Returns the number of elements in this set.
	 * 
	 * @return the number of elements in this set.
	 */
	public int size() {
		if (elementCount == 0)
			return 0;
		expungeStaleEntries();
		return elementCount;
	}

	// ========== Internal Entry

	private static final class Entry<T> extends WeakReference<T> {
		private final int hash;
		private Entry<T> nextInSlot;

		private Entry(final T value, ReferenceQueue<T> queue) {
			super(value, queue);
			hash = (value.hashCode() & 0x7FFFFFFF);
		}
	}

	// ========== Prime Finder

	private static final int primeSize(final int capacity) {
		return PrimeFinder.nextPrime(capacity);
	}

}
