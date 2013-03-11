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
package org.kvstore.structures.set;

import java.util.Arrays;

import org.apache.log4j.Logger;

/**
 * Native Int SortedArray
 * This class is NOT Thread-Safe
 *
 * @author Guillermo Grandes / guillermo.grandes[at]gmail.com
 */
public class SortedIntArraySet {
	public static final int NULL_VALUE = Integer.MIN_VALUE;
	private static final Logger log = Logger.getLogger(SortedIntArraySet.class);
	//
	public int[] keys;
	public int allocated = 0;
	/**
	 * Create with initial size
	 * @param size
	 */
	public SortedIntArraySet(final int size) {
		allocArray(size);
	}
	/**
	 * Alloc array
	 * @param size
	 */
	private final void allocArray(final int size) {
		keys = new int[size];
	}
	/**
	 * Resize array
	 */
	private final void resizeArray() {
		if (log.isDebugEnabled())
			log.debug("resizeArray size=" + keys.length + " newsize=" + (keys.length << 1));
		final int[] newkeys = new int[keys.length << 1]; // double space
		System.arraycopy(keys, 0, newkeys, 0, allocated);
		keys = newkeys;
	}
	/**
	 * Find slot by key
	 * @param searchKey
	 * @return
	 */
	private final int findSlotByKey(final int searchKey) {
		return Arrays.binarySearch(keys, 0, allocated, searchKey);
	}
	/**
	 * Is empty?
	 * @return
	 */
	public boolean isEmpty() { // empty
		return (allocated <= 0);
	}
	/**
	 * Is full?
	 * @return
	 */
	private final boolean isFull() { // full
		if (log.isDebugEnabled())
			log.debug("allocated=" + allocated + " keys.length=" + keys.length);
		return (allocated >= keys.length);
	}
	/**
	 * Clear all elements
	 */
	public void clear() {
		Arrays.fill(keys, NULL_VALUE);
		allocated = 0;
	}
	/**
	 * insert element
	 */
	private final void moveElementsRight(final int[] elements, final int srcPos) {
		if (log.isDebugEnabled())
			log.debug("moveElementsRight(" + srcPos + ") allocated=" + allocated + ":" + keys.length + ":" + (allocated - srcPos) + ":" + (keys.length - srcPos - 1));
		System.arraycopy(elements, srcPos, elements, srcPos + 1, (allocated - srcPos));
	}
	/**
	 * remove element
	 */
	private final void moveElementsLeft(final int[] elements, final int srcPos) {
		if (log.isDebugEnabled())
			log.debug("moveElementsLeft(" + srcPos + ") allocated=" + allocated + ":" + keys.length + ":" + (allocated - srcPos - 1) + ":" + (keys.length - srcPos - 1));
		System.arraycopy(elements, srcPos + 1, elements, srcPos, (allocated - srcPos - 1));
	}
	/**
	 * remove slot
	 * @param slot
	 * @return
	 */
	private final boolean removeSlot(final int slot) {
		if (slot < 0) {
			log.error("faking slot=" + slot + " allocated=" + allocated);
			return false;
		}
		if (slot < allocated) {
			moveElementsLeft(keys, slot);
		}
		if (allocated > 0)
			allocated--;
		if (log.isDebugEnabled())
			log.debug("erased up key=" + keys[allocated]);
		keys[allocated] = NULL_VALUE;
		return true;
	}
	/**
	 * remove key
	 * @param key
	 * @return
	 */
	public boolean remove(final int key) {
		int slot = findSlotByKey(key);
		if (slot >= 0) {
			return removeSlot(slot);
		}
		return false;
	}
	/**
	 * put key
	 * @param key
	 * @return
	 */
	public boolean put(final int key) {
		if (isFull()) { // full
			resizeArray();
		}
		int slot = findSlotByKey(key);
		if (slot >= 0) {
			if (log.isDebugEnabled())
				log.debug("key already exists: " + key);
			return false; // key already exist
		}
		slot = ((-slot) - 1);
		return addSlot(slot, key);
	}
	/**
	 * add slot
	 * @param slot
	 * @param key
	 * @return
	 */
	private final boolean addSlot(final int slot, final int key) {
		if (slot < allocated) {
			moveElementsRight(keys, slot);
		}
		allocated++;
		keys[slot] = key;
		return true;
	}
	/**
	 * Returns the first (lowest) element currently in this set.
	 */
	public int first() {
		return keys[0];
	}
	/**
	 * Returns the last (highest) element currently in this set.
	 */
	public int last() {
		if (allocated == 0)
			return NULL_VALUE;
		return keys[allocated-1];
	}
	/**
	 * Returns the greatest element in this set less than or equal to the given element, or NULL_VALUE if there is no such element.	
	 * @param key
	 */
	public int floor(final int key) {
		return getRoundKey(key, false, true);
	}
	/**
	 * Returns the least element in this set greater than or equal to the given element, or NULL_VALUE if there is no such element.
	 * @param key
	 */
	public int ceiling(final int key) {
		return getRoundKey(key, true, true);
	}
	/**
	 * Returns the greatest element in this set strictly less than the given element, or NULL_VALUE if there is no such element.
	 * @param key
	 */
	public int lower(final int key) {
		return getRoundKey(key, false, false);
	}
	/**
	 * Returns the least element in this set strictly greater than the given element, or NULL_VALUE if there is no such element.
	 * @param key
	 */
	public int higher(final int key) {
		return getRoundKey(key, true, false);
	}
	/**
	 * find key
	 * @param key
	 * @param upORdown
	 * @param acceptEqual
	 * @return
	 */
	private final int getRoundKey(final int key, final boolean upORdown, final boolean acceptEqual) {
		if (isEmpty()) return NULL_VALUE;
		int slot = findSlotByKey(key);
		if (upORdown) {
			// ceiling / higher
			slot = ((slot < 0) ? (-slot)-1 : (acceptEqual ? slot : slot+1));
			if (slot >= allocated) {
				return NULL_VALUE;
			}
		}
		else {
			// floor / lower
			slot = ((slot < 0) ? (-slot)-2 : (acceptEqual ? slot : slot-1));
			if (slot < 0) {
				return NULL_VALUE;
			}
		}
		return keys[slot];
	}	
	/**
	 * Returns true if this set contains the specified element.
	 * @param key
	 * @return
	 */
	public boolean contains(final int key) {
		int slot = findSlotByKey(key);
		return (slot >= 0);
	}
	/**
	 * Returns the element at the specified position in his internal array.
	 * @param slot
	 * @return
	 */
	public int get(final int slot) {
		if ((slot < 0) || (slot >= allocated))
			return NULL_VALUE;
		return keys[slot];
	}
	/**
	 * Returns the number of elements in this set (its cardinality).
	 * @return
	 */
	public int size() {
		return allocated;
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();
		sb.append("[");
		for (int i = 0; i < allocated; i++) {
			final int k = keys[i];
			sb.append(k).append("|");
		}
		if (allocated > 0)
			sb.setLength(sb.length() - 1);
		sb.append("]");
		return sb.toString();
	}

	public IntIterator iterator() {
		return new IntIterator(this);
	}

	static class IntIterator {
		final SortedIntArraySet associatedSet;
		int nextEntry = 0;
		int lastReturned = -1;

		public IntIterator(final SortedIntArraySet associatedSet) {
			this.associatedSet = associatedSet;
			nextEntry = 0;
		}

		public boolean hasNext() {
			return (nextEntry < associatedSet.allocated);
		}

		public void remove() {
			if (lastReturned == -1)
				throw new IllegalStateException();

			associatedSet.removeSlot(lastReturned);
			lastReturned = -1;
		}

		public int next() {
			lastReturned = nextEntry;
			return associatedSet.keys[nextEntry++];
		}
	}

	/**
	 * Test
	 * @param args
	 */
	public static void main(final String[] args) {
		SortedIntArraySet s = new SortedIntArraySet(3);
		s.put(90);
		s.put(10);
		s.put(20);
		s.put(30);
		System.out.println("toString()=" + s.toString());
		s.remove(10);
		s.put(40);
		System.out.println("toString()=" + s.toString());
		System.out.println("first=" + s.first());
		System.out.println("last()=" + s.last());
		System.out.println("floor(15)=" + s.floor(15));
		System.out.println("ceiling(15)=" + s.ceiling(15));
		System.out.println("lower(15)=" + s.lower(15));
		System.out.println("higher(15)=" + s.higher(15));
		System.out.println("floor(20)=" + s.floor(20));
		System.out.println("ceiling(20)=" + s.ceiling(20));
		System.out.println("lower(20)=" + s.lower(20));
		System.out.println("higher(20)=" + s.higher(20));
		System.out.println("floor(0)=" + s.floor(0));
		System.out.println("ceiling(0)=" + s.ceiling(0));
		System.out.println("lower(0)=" + s.lower(0));
		System.out.println("higher(0)=" + s.higher(0));
		System.out.println("floor(9999)=" + s.floor(9999));
		System.out.println("ceiling(9999)=" + s.ceiling(9999));
		System.out.println("lower(9999)=" + s.lower(9999));
		System.out.println("higher(9999)=" + s.higher(9999));
		System.out.println("contains(20)=" + s.contains(20));
		System.out.println("contains(9999)=" + s.contains(9999));
		System.out.println("size()=" + s.size());
		System.out.println("-------- iter begin");
		IntIterator iter = s.iterator();
		while (iter.hasNext()) {
			System.out.println(iter.next());
		}
		System.out.println("-------- iter end");
		System.out.println("-------- for begin");
		for (int i = -1; i <= s.size(); i++) {
			System.out.println(s.get(i));
		}
		System.out.println("-------- for end");
		s.clear();
		System.out.println("first=" + s.first());
		System.out.println("last()=" + s.last());
	}

}
