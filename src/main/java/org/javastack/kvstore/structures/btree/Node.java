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
package org.javastack.kvstore.structures.btree;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.log4j.Logger;
import org.javastack.kvstore.holders.DataHolder;

/**
 * Generic Node definition
 * This class is NOT Thread-Safe
 * 
 * @param <K>
 * @param <V>
 * 
 * @author Guillermo Grandes / guillermo.grandes[at]gmail.com
 */
public abstract class Node<K extends DataHolder<K>, V extends DataHolder<V>> {
	private static final Logger log = Logger.getLogger(Node.class);
	public static final int NULL_ID = 0;

	public final BplusTree<K, V> tree;
	public final K[] keys;

	public int id = Node.NULL_ID;
	public int allocated = 0;

	protected Node(final BplusTree<K, V> tree) {
		this.tree = tree;
		this.keys = tree.getGenericFactoryK().newArray(getBOrder());
	}

	public int allocId() {
		id = tree.allocNode(isLeaf());
		// Dont do this here, do after allocId or after split
		// tree.putNode(this);
		return id;
	}

	public static boolean isLeaf(final int id) {
		return (id > 0);
	}

	/**
	 * Searches using the binary search algorithm. {@link Arrays#binarySearch(Object[], int, int, Object)}
	 * 
	 * @param key the value to be searched for
	 * @return index of the search key, if it is contained in the array
	 *         within the specified range;
	 *         otherwise, <tt>(-(<i>insertion point</i>) - 1)</tt>. The
	 *         <i>insertion point</i> is defined as the point at which the
	 *         key would be inserted into the array: the index of the first
	 *         element in the range greater than the key,
	 *         or <tt>toIndex</tt> if all
	 *         elements in the range are less than the specified key. Note
	 *         that this guarantees that the return value will be &gt;= 0 if
	 *         and only if the key is found.
	 */
	public final int findSlotByKey(final K searchKey) {
		// return Arrays.binarySearch(keys, 0, allocated, searchKey);
		int low = 0;
		int high = allocated - 1;

		while (low <= high) {
			final int mid = (low + high) >>> 1;
			final K midVal = keys[mid];
			final int cmp = midVal.compareTo(searchKey);

			if (cmp < 0) {
				low = mid + 1;
			} else if (cmp > 0) {
				high = mid - 1;
			} else {
				return mid; // key found
			}
		}
		return -(low + 1);  // key not found.
	}

	public boolean isEmpty() { // node empty
		return (allocated <= 0);
	}

	public boolean isFull() { // node is full
		if (log.isDebugEnabled())
			log.debug("allocated=" + allocated + " keys.length=" + keys.length);
		return (allocated >= keys.length);
	}

	public boolean isUnderFlow() {
		return (allocated < (keys.length >> 1));
	}

	public boolean canMerge(final Node<K, V> other) {
		return ((allocated + other.allocated + 1) < keys.length); // TODO: revisar el +1
	}

	protected void clear() {
		Arrays.fill(keys, null);
		allocated = 0;
	}

	protected void delete() {
		clear();
		allocated = Integer.MIN_VALUE;
	}

	protected boolean isDeleted() {
		return (allocated == Integer.MIN_VALUE);
	}

	// insert element
	protected void moveElementsRight(final Object[] elements, final int srcPos) {
		if (log.isDebugEnabled()) {
			log.debug("moveElementsRight(" + srcPos + ") allocated=" + allocated + ":" + keys.length + ":"
					+ (allocated - srcPos) + ":" + (keys.length - srcPos - 1));
		}
		System.arraycopy(elements, srcPos, elements, srcPos + 1, allocated - srcPos);
	}

	// remove element
	protected void moveElementsLeft(final Object[] elements, final int srcPos) {
		if (log.isDebugEnabled()) {
			log.debug("moveElementsLeft(" + srcPos + ") allocated=" + allocated + ":" + keys.length + ":"
					+ (allocated - srcPos - 1) + ":" + (keys.length - srcPos - 1));
		}
		System.arraycopy(elements, srcPos + 1, elements, srcPos, allocated - srcPos - 1);
	}

	public int countKeys() {
		int low = 0, high = keys.length;
		while (high != low) {
			int middle = (high + low) / 2;
			if (keys[middle] == null) {
				high = middle;
			} else {
				low = middle + 1;
			}
		}
		return low;
	}

	// ========= Serialization =========

	public int getStructMaxSize() {
		final K factoryK = tree.factoryK();
		return (4 + 2 + (keys.length * factoryK.byteLength()));
	}

	public int getStructEstimateSize(final int b) {
		final K factoryK = tree.factoryK();
		return (4 + 2 + (b * factoryK.byteLength()));
	}

	public void serialize(final ByteBuffer buf) {
		buf.clear();
		buf.putInt(id);								// 4 bytes
		buf.putShort((short) (allocated & 0x7FFF));	// 2 bytes
		for (int i = 0; i < allocated; i++) {		// X bytes * b_order
			keys[i].serialize(buf);
		}
	}

	public final void clean(final ByteBuffer buf) {
		buf.clear();
		// buf.putInt(0); // 4 bytes
		// buf.putShort((short) 0); // 2 bytes
		buf.putLong(0);				// 8 bytes
		buf.flip();
	}

	public static <K extends DataHolder<K>, V extends DataHolder<V>> Node<K, V> deserialize(
			final ByteBuffer buf, final BplusTree<K, V> tree) {
		final int id = buf.getInt();
		if (id == NULL_ID) {
			throw InvalidNodeID.NULL_ID;
		}
		final boolean isLeaf = isLeaf(id);
		final Node<K, V> node = (isLeaf ? tree.createLeafNode() : tree.createInternalNode());
		node.id = id;
		return node.deserializeNode(buf);
	}

	protected Node<K, V> deserializeNode(final ByteBuffer buf) {
		final K factoryK = tree.factoryK();
		allocated = buf.getShort();
		// Arrays.fill(keys, null);
		for (int i = 0; i < allocated; i++) {
			keys[i] = factoryK.deserialize(buf);
		}
		return this;
	}

	// ========= Abstract =========

	abstract public boolean remove(int slot);

	abstract public boolean isLeaf();

	abstract public Node<K, V> split();

	abstract public int getBOrder();

	abstract public boolean isFreeable();

	abstract public K splitShiftKeysLeft();

	/**
	 * Merge two nodes, this node will absorb nodeFROM
	 * 
	 * @param nodeParent a node parent for this & nodeFROM
	 * @param nodeFROM a node (will be clean)
	 */
	abstract protected void merge(final InternalNode<K, V> nodeParent, final int slot,
			final Node<K, V> nodeFROM);

	/**
	 * Shift keys from nodeFROM (left) into this node (right)
	 * 
	 * @param nodeParent the parent of nodeFROM and this node
	 * @param slot the index nodeTO in nodeParent.childs
	 * @param nodeFROM the right sibling of nodeTO
	 */
	abstract protected void shiftLR(final InternalNode<K, V> nodeParent, final int slot,
			final Node<K, V> nodeFROM);

	/**
	 * Shift keys from node nodeFROM (right) into this node (left)
	 * 
	 * @param nodeParent the parent of nodeFROM and this node
	 * @param slot the index nodeTO in nodeParent.childs
	 * @param nodeFROM the left sibling of nodeTO
	 */
	abstract protected void shiftRL(final InternalNode<K, V> nodeParent, final int slot,
			final Node<K, V> nodeFROM);

	public static class InvalidNodeID extends RuntimeException {
		private static final long serialVersionUID = 42L;
		public static final InvalidNodeID NULL_ID = new InvalidNodeID("Invalid Node id=NULL_ID");

		public InvalidNodeID(final String error) {
			super(error);
		}
	}
}
