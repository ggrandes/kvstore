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
package org.javastack.kvstore.structures.hash;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.log4j.Logger;
import org.javastack.kvstore.utils.GenericFactory;
import org.javastack.kvstore.utils.PrimeFinder;

/**
 * Native Integer LinkedHashMap
 * This class is NOT Thread-Safe
 * @param <V> type of values
 *
 * @author Guillermo Grandes / guillermo.grandes[at]gmail.com
 */
public class IntLinkedHashMap<V> implements Iterable<V> {
	private static final Logger log = Logger.getLogger(IntLinkedHashMap.class);

	private int elementCount;
	private IntLinkedEntry<V>[] elementData;

	private final float loadFactor;
	private int threshold;
	private int defaultSize = 17;

	private GenericFactory<V> factory;

	/**
	 * The head of the doubly linked list.
	 */
	private transient IntLinkedEntry<V> header;

	/**
	 * The iteration ordering method for this linked hash map: <tt>true</tt>
	 * for access-order, <tt>false</tt> for insertion-order.
	 *
	 * @serial
	 */
	private final boolean accessOrder;

	/**
	 * Constructs a new {@code IntLinkedHashMap} instance with the specified capacity.
	 *
	 * @param capacity the initial capacity of this hash map.
	 * @throws IllegalArgumentException when the capacity is less than zero.
	 */
	public IntLinkedHashMap(final Class<V> type) {
		this(17, type, false);
	}
	public IntLinkedHashMap(final int capacity, final Class<V> type) {
		this(capacity, type, false);
	}
	public IntLinkedHashMap(final int capacity, final Class<V> type, final boolean accessOrder) {
		this.accessOrder = accessOrder;
		//
		factory = new GenericFactory<V>(type);
		defaultSize = primeSize(capacity);
		if (capacity >= 0) {
			elementCount = 0;
			elementData = newElementArray(defaultSize);
			loadFactor = 0.75f; // Default load factor of 0.75
			initCache(elementData.length);
			computeMaxSize();
		} else {
			throw new IllegalArgumentException();
		}
		// Initializes the chain.
		initChain();
	}

	@SuppressWarnings("unchecked")
	private IntLinkedEntry<V>[] newElementArray(int s) {
		return new IntLinkedEntry[s];
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

	public void clear(final boolean shrink) {
		clearCache();
		if (elementCount > 0) {            
			elementCount = 0;            
		}
		if (shrink && (elementData.length > 1024) && (elementData.length > defaultSize)) {
			elementData = newElementArray(defaultSize);
		}
		else {
			Arrays.fill(elementData, null);
		}
		computeMaxSize();
		initChain();
	}

	private void initChain() {
		// Initializes the chain.
		header = new IntLinkedEntry<V>(-1);
		header.before = header.after = header;
	}

	private void computeMaxSize() {
		threshold = (int) (elementData.length * loadFactor);
	}


	/**
	 * Returns the value of the mapping with the specified key.
	 *
	 * @param key the key.
	 * @return the value of the mapping with the specified key, or {@code null}
	 *         if no mapping for the specified key is found.
	 */
	public V get(final int key) {
		final int index = (key & 0x7FFFFFFF) % elementData.length;

		IntLinkedEntry<V> m = elementData[index];
		while (m != null) {
			if (key == m.key) {
				if (accessOrder) {
					//if (log.isDebugEnabled()) log.debug("reliking " + this.key);
					m.remove();
					m.addBefore(header);
				}
				return m.value;
			}
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

		IntLinkedEntry<V> entry = elementData[index];
		while (entry != null && key != entry.key) {
			entry = entry.nextInSlot;
		}

		if (entry == null) {
			// Remove eldest entry if instructed, else grow capacity if appropriate
			IntLinkedEntry<V> eldest = header.after;
			++elementCount;
			if (removeEldestEntry(eldest)) {
				remove(eldest.key);
			} else {
				if (elementCount > threshold) {
					rehash();
					index = (key & 0x7FFFFFFF) % elementData.length;
				}
			}
			entry = createHashedEntry(key, index);
		}

		V result = entry.value;
		entry.value = value;
		return result;
	}

	IntLinkedEntry<V> createHashedEntry(final int key, final int index) {
		IntLinkedEntry<V> entry = reuseAfterDelete();
		if (entry == null) {
			entry = new IntLinkedEntry<V>(key);
		} 
		else {
			entry.key = key;
			entry.value = null;
		}

		entry.nextInSlot = elementData[index];
		elementData[index] = entry;
		entry.addBefore(header); // LinkedList
		return entry;
	}

	void rehash(final int capacity) {
		final int length = primeSize(capacity == 0 ? 1 : capacity << 1);
		if (log.isDebugEnabled()) log.debug(this.getClass().getName() + "::rehash() old=" + elementData.length + " new=" + length);

		IntLinkedEntry<V>[] newData = newElementArray(length);
		for (int i = 0; i < elementData.length; i++) {
			IntLinkedEntry<V> entry = elementData[i];
			while (entry != null) {
				int index = (entry.key & 0x7FFFFFFF) % length;
				IntLinkedEntry<V> next = entry.nextInSlot;
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
		IntLinkedEntry<V> entry = removeEntry(key);
		if (entry == null)
			return null;
		V ret = entry.value;
		reuseAfterDelete(entry);

		return ret;
	}

	public V removeEldest() {
		final IntLinkedEntry<V> eldest = header.after;
		V ret = eldest.value;
		remove(eldest.key);
		return ret;
	}

	IntLinkedEntry<V> removeEntry(final int key) {
		IntLinkedEntry<V> last = null;

		final int index = (key & 0x7FFFFFFF) % elementData.length;
		IntLinkedEntry<V> entry = elementData[index];

		while (true) {
			if (entry == null)
				return null;

			if (key == entry.key) {
				if (last == null) {
					elementData[index] = entry.nextInSlot;
				} else {
					last.nextInSlot = entry.nextInSlot;
				}
				--elementCount;
				entry.remove();
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

	private ArrayDeque<IntLinkedEntry<V>> cache;

	private void initCache(final int size) {
		cache = new ArrayDeque<IntLinkedEntry<V>>(size);
	}
	public void clearCache() { 
		cache.clear();
	}
	private IntLinkedEntry<V> reuseAfterDelete() {
		final IntLinkedEntry<V> reuse = cache.pollLast();
		/*if (reuse != null) {
			if (log.isDebugEnabled()) log.debug("reusing IntLinkedEntry<V>=" + reuse.hashCode() + " cacheSize=" + cache.size());
		}*/
		return reuse;
	}
	private void reuseAfterDelete(final IntLinkedEntry<V> entry) {
		entry.clean();
		cache.offerLast(entry);
	}

	// ========== Internal Entry

	protected static final class IntLinkedEntry<V> {
		// These fields comprise the doubly linked list used for iteration.
		private IntLinkedEntry<V> before, after;
		//
		private IntLinkedEntry<V> nextInSlot;
		protected int key;
		protected V value;

		IntLinkedEntry(final int theKey) {
			this.key = theKey;
			this.value = null;
		}

		private void clean() {
			value = null;
			key = Integer.MIN_VALUE;
			nextInSlot = null;
			before = null;
			after = null;
		}

		/**
		 * Removes this entry from the linked list.
		 */
		private void remove() {
			before.after = after;
			after.before = before;
		}

		/**
		 * Inserts this entry before the specified existing entry in the list.
		 */
		private void addBefore(IntLinkedEntry<V> existingEntry) {
			after  = existingEntry;
			before = existingEntry.before;
			before.after = this;
			after.before = this;
		}

		/**
		 * Returns the key corresponding to this entry.
		 *
		 * @return the key corresponding to this entry
		 */
		public int getKey() {
			return key;
		}
		
		/**
		 * Returns the value corresponding to this entry.
		 *
		 * @return the value corresponding to this entry
		 */
		public V getValue() {
			return value;
		}
	}

	// ========== Linked List

	/**
	 * Returns <tt>true</tt> if this map should remove its eldest entry.
	 * This method is invoked by <tt>put</tt> and <tt>putAll</tt> after
	 * inserting a new entry into the map.  It provides the implementor
	 * with the opportunity to remove the eldest entry each time a new one
	 * is added.  This is useful if the map represents a cache: it allows
	 * the map to reduce memory consumption by deleting stale entries.
	 *
	 * <p>Sample use: this override will allow the map to grow up to 100
	 * entries and then delete the eldest entry each time a new entry is
	 * added, maintaining a steady state of 100 entries.
	 * <pre>
	 *     private static final int MAX_ENTRIES = 100;
	 *
	 *     protected boolean removeEldestEntry(IntLinkedEntry eldest) {
	 *        return size() > MAX_ENTRIES;
	 *     }
	 * </pre>
	 *
	 * <p>This method typically does not modify the map in any way,
	 * instead allowing the map to modify itself as directed by its
	 * return value.  It <i>is</i> permitted for this method to modify
	 * the map directly, but if it does so, it <i>must</i> return
	 * <tt>false</tt> (indicating that the map should not attempt any
	 * further modification).  The effects of returning <tt>true</tt>
	 * after modifying the map from within this method are unspecified.
	 *
	 * <p>This implementation merely returns <tt>false</tt> (so that this
	 * map acts like a normal map - the eldest element is never removed).
	 *
	 * @param    eldest The least recently inserted entry in the map, or if
	 *           this is an access-ordered map, the least recently accessed
	 *           entry.  This is the entry that will be removed it this
	 *           method returns <tt>true</tt>.  If the map was empty prior
	 *           to the <tt>put</tt> or <tt>putAll</tt> invocation resulting
	 *           in this invocation, this will be the entry that was just
	 *           inserted; in other words, if the map contains a single
	 *           entry, the eldest entry is also the newest.
	 * @return   <tt>true</tt> if the eldest entry should be removed
	 *           from the map; <tt>false</tt> if it should be retained.
	 */
	protected boolean removeEldestEntry(IntLinkedEntry<V> eldest) {
		return false;
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
		return new IntLinkedHashMapIterator<V>(this);
	}

	static class IntLinkedHashMapIterator<V> implements Iterator<V> {
		final IntLinkedHashMap<V> associatedMap;
		IntLinkedEntry<V> nextEntry    = null;
		IntLinkedEntry<V> lastReturned = null;

		public IntLinkedHashMapIterator(final IntLinkedHashMap<V> associatedMap) {
			this.associatedMap = associatedMap;
			nextEntry = associatedMap.header.after;
		}

		public boolean hasNext() {
			return nextEntry != associatedMap.header;
		}

		public void remove() {
			if (lastReturned == null)
				throw new IllegalStateException();

			associatedMap.remove(lastReturned.key);
			lastReturned = null;
		}

		IntLinkedEntry<V> nextEntry() {
			if (nextEntry == associatedMap.header)
				throw new NoSuchElementException();

			IntLinkedEntry<V> e = lastReturned = nextEntry;
			nextEntry = e.after;
			return e;
		}
		public V next() { return nextEntry().value; }
	}

	// =========================================

	public V[] getValues() {
		final V[] array = factory.newArray(elementCount);
		int i = 0;
		for (final V v : this) {
			array[i++] = v;
		}
		return array;
	}

	// =========================================

	public static void main(String[] args) {
		IntLinkedHashMap<Integer> hash = new IntLinkedHashMap<Integer>(16, Integer.class, true) {
			/*protected boolean removeEldestEntry(IntLinkedEntry<Integer> eldest) {
				System.out.println("---- begin");
				for (Integer i : this) {
					System.out.println(i);
				}
				System.out.println("---- end");
				return (size() > 3);
	        }*/
		};
		for (int i = 1; i < 6; i++) { // 1...4
			hash.put(i, i);
		}
		hash.put(3, 3);
		hash.put(3, 3);
		hash.put(3, 3);
		hash.put(3, 3);
		hash.get(3);
		//hash.remove(3);
		for (Integer i : hash) {
			System.out.println(i);
		}
		System.out.println("---");
		while (hash.size() > 0) {
			System.out.println("remove value=" + hash.removeEldest());
		}
		System.out.println("---");

		for (Integer i : hash) {
			System.out.println(i);
		}
	}
}
