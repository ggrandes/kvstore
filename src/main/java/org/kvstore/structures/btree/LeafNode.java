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
package org.kvstore.structures.btree;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.kvstore.holders.DataHolder;

/**
 * Leaf Node of BplusTree<K, V>
 * This class is NOT Thread-Safe
 *
 * @param <K> key type (DataHolder<K>)
 * @param <V> value type (DataHolder<V>)
 *
 * @author Guillermo Grandes / guillermo.grandes[at]gmail.com
 */
public final class LeafNode<K extends DataHolder<K>, V extends DataHolder<V>> extends Node<K, V> {
	public final V[] values;
	public int leftid = NULL_ID;
	public int rightid = NULL_ID;

	protected LeafNode(final BplusTree<K, V> tree) {
		super(tree);
		this.values = tree.getGenericFactoryV().newArray(getBOrder());
	}

	@Override
	public int getBOrder() {
		return tree.getBOrderLeaf();
	}
	@Override
	public boolean isLeaf() {
		return true;
	}
	@Override
	public boolean isFreeable() {
		return isEmpty() && (values[0] == null);
	}
	@Override
	public void clear() {
		super.clear();
		leftid = rightid = NULL_ID;
		Arrays.fill(values, null);
	}

	public V set(final int slot, final V newValue) {
		final V oldValue = values[slot];
		values[slot] = newValue;
		return oldValue;
	}

	public boolean add(final K newKey, final V newValue) {
		if (isFull()) { // node is full
			if (DEBUG) System.out.println("overflow");
			return false;
		}
		// TODO: Reparar
		int slot = findSlotByKey(newKey); 
		if (slot >= 0) {
			if (DEBUG) System.out.println("key already exists: " + newKey);
			return false; // key already exist
		}
		slot = (-slot)-1;
		return add(slot, newKey, newValue);
	}

	public boolean add(final int slot, final K newKey, final V newValue) {
		//if (DEBUG) System.out.println("add("+newKey+") i=" + slot);
		if (slot < allocated) { 
			moveElementsRight(keys, slot);
			moveElementsRight(values, slot);
		}
		allocated++;
		keys[slot] = newKey;
		values[slot] = newValue;
		return true;
	}
	@Override
	public boolean remove(final int slot) {
		if (slot < 0) {
			System.out.println("faking slot=" + slot + " allocated=" + allocated);
			return false;
		}
		if (slot < allocated) { 
			moveElementsLeft(keys, slot);
			moveElementsLeft(values, slot);
		}
		if (allocated > 0) allocated--;
		if (DEBUG) System.out.println("erased up key=" + keys[allocated] + " value="+ values[allocated]);
		keys[allocated] = null;
		values[allocated] = null;
		return true;
	}

	@Override
	public LeafNode<K, V> split() {
		final LeafNode<K, V> newHigh = tree.createLeafNode();
		newHigh.allocId();
		//int j = ((allocated >> 1) | (allocated & 1)); // dividir por dos y sumar el resto (0 o 1)
		int j = (allocated >> 1); // dividir por dos (libro)
		final int newsize = allocated-j;
		//if (DEBUG) System.out.println("split j=" + j);
		System.arraycopy(keys, j, newHigh.keys, 0, newsize);
		System.arraycopy(values, j, newHigh.values, 0, newsize);
		// Limpiar la parte alta de los arrays de referencias inutiles
		for (int i = j; i < j + newsize; i++) {
			keys[i] = null;
			values[i] = null;
		}
		newHigh.allocated = newsize;
		allocated -= newsize;
		// Update Linked List (left) in old High
		if (rightid != NULL_ID) {
			final LeafNode<K, V> oldHigh = (LeafNode<K, V>)tree.getNode(rightid);
			oldHigh.leftid = newHigh.id;
			tree.putNode(oldHigh);
		}
		// Linked List (left) in new High
		newHigh.leftid = id;
		// Linked List (right)
		newHigh.rightid = rightid;
		rightid = newHigh.id; 
		// update lowIdx on tree
		if (leftid == 0) tree.lowIdx = id;
		// update highIdx on tree
		if (newHigh.rightid == 0) tree.highIdx = newHigh.id;
		//
		tree.putNode(this);
		tree.putNode(newHigh);
		return newHigh;
	}
	@Override
	public K splitShiftKeysLeft() {
		return keys[0];
	}

	/**
	 * Return the previous LeafNode in LinkedList 
	 * @return LeafNode<K, V> previous node
	 */
	public LeafNode<K, V> prevNode() {
		if (leftid == NULL_ID) return null;
		return (LeafNode<K, V>)tree.getNode(leftid);
	}

	/**
	 * Return the next LeafNode in LinkedList 
	 * @return LeafNode<K, V> next node
	 */
	public LeafNode<K, V> nextNode() {
		if (rightid == NULL_ID) return null;
		return (LeafNode<K, V>)tree.getNode(rightid);
	}

	public String toString() {
		final StringBuilder sb = new StringBuilder();
		sb.append(leftid).append("<<");
		sb.append("[").append("L").append(id).append("]");
		sb.append(">>").append(rightid);
		sb.append("(").append(allocated).append("){");
		for (int i = 0; i < allocated; i++) {
			final K k = keys[i];
			final V v = values[i];
			sb.append(k).append("=").append(v).append("|");
		}
		if (allocated > 0) sb.setLength(sb.length()-1);
		sb.append("}");
		return sb.toString();
	}

	// ========= Remove Helpers =========

	protected final void merge(final InternalNode<K, V> nodeParent, final int slot, final Node<K, V> nodeFROMx) {
		final LeafNode<K, V> nodeFROM = (LeafNode<K, V>)nodeFROMx;
		final LeafNode<K, V> nodeTO = this;
		final int sizeTO = nodeTO.allocated;
		final int sizeFROM = nodeFROM.allocated;
		// copy keys from nodeFROM to nodeTO
		System.arraycopy(nodeFROM.keys, 0, nodeTO.keys, sizeTO, sizeFROM);
		System.arraycopy(nodeFROM.values, 0, nodeTO.values, sizeTO, sizeFROM);
		nodeTO.allocated += sizeFROM;
		// remove key from nodeParent
		nodeParent.remove(slot);
		// Update Linked List (left) in new High
		if (nodeFROM.rightid != NULL_ID) {
			final LeafNode<K, V> rightFROM = (LeafNode<K, V>)tree.getNode(nodeFROM.rightid);
			rightFROM.leftid = id;
			tree.putNode(rightFROM);
		}
		// Update Linked List (right)
		rightid = nodeFROM.rightid;
		// update lowIdx on tree
		if (leftid == 0) tree.lowIdx = id;
		// update highIdx on tree
		if (rightid == 0) tree.highIdx = id;
		// 
		// Free nodeFROM
		tree.freeNode(nodeFROM);
	}

	protected final void shiftLR(final InternalNode<K, V> nodeParent, final int slot, final Node<K, V> nodeFROMx) {
		final LeafNode<K, V> nodeFROM = (LeafNode<K, V>)nodeFROMx;
		final LeafNode<K, V> nodeTO = this;
		final int sizeTO = nodeTO.allocated;
		final int sizeFROM = nodeFROM.allocated;
		final int shift = ((sizeTO+sizeFROM)/2) - sizeTO;  // num. keys to shift from nodeFROM to nodeTO
		// make space for new keys in nodeTO
		System.arraycopy(nodeTO.keys, 0, nodeTO.keys, shift, sizeTO);
		System.arraycopy(nodeTO.values, 0, nodeTO.values, shift, sizeTO);
		// move keys and children out of nodeFROM and into nodeTO (and nodeU)
		nodeTO.keys[shift-1] = nodeParent.keys[slot];
		nodeParent.keys[slot] = nodeFROM.keys[sizeFROM-shift];
		System.arraycopy(nodeFROM.keys, sizeFROM-shift, nodeTO.keys, 0, shift);
		Arrays.fill(nodeFROM.keys, sizeFROM-shift, sizeFROM, null);
		System.arraycopy(nodeFROM.values, sizeFROM-shift, nodeTO.values, 0, shift);
		Arrays.fill(nodeFROM.values, sizeFROM-shift, sizeFROM, null);
		nodeTO.allocated += shift;
		nodeFROM.allocated -= shift;
	}

	protected final void shiftRL(final InternalNode<K, V> nodeParent, final int slot, final Node<K, V> nodeFROMx) {
		final LeafNode<K, V> nodeFROM = (LeafNode<K, V>)nodeFROMx;
		final LeafNode<K, V> nodeTO = this;
		final int sizeTO = nodeTO.allocated;
		final int sizeFROM = nodeFROM.allocated;
		final int shift = ((sizeTO+sizeFROM)/2) - sizeTO;  // num. keys to shift from nodeFROM to nodeTO
		// shift keys and children from nodeFROM to nodeTO
		nodeTO.keys[sizeTO] = nodeParent.keys[slot];
		System.arraycopy(nodeFROM.keys, 0, nodeTO.keys, sizeTO, shift);
		System.arraycopy(nodeFROM.values, 0, nodeTO.values, sizeTO, shift);
		nodeParent.keys[slot] = nodeFROM.keys[shift];
		// delete keys and children from nodeFROM
		System.arraycopy(nodeFROM.keys, shift, nodeFROM.keys, 0, sizeFROM-shift);
		Arrays.fill(nodeFROM.keys, sizeFROM-shift, sizeFROM, null);
		System.arraycopy(nodeFROM.values, shift, nodeFROM.values, 0, sizeFROM-shift);
		Arrays.fill(nodeFROM.values, sizeFROM-shift, sizeFROM, null);
		nodeTO.allocated += shift;
		nodeFROM.allocated -= shift;
	}

	// ========= Serialization =========

	@Override
	public int getStructMaxSize() {
		final V factoryV = tree.factoryV();
		return super.getStructMaxSize() + (values.length * factoryV.byteLength()) + 4 + 4;
	}
	@Override
	public int getStructEstimateSize(final int b) {
		final V factoryV = tree.factoryV();
		return super.getStructEstimateSize(b) + (b * factoryV.byteLength()) + 4 + 4;
	}
	@Override
	public void serialize(final ByteBuffer buf) {
		super.serialize(buf);
		for (int i = 0; i < allocated; i++) {
			values[i].serialize(buf);
		}
		buf.putInt(leftid);		// 4 bytes
		buf.putInt(rightid);	// 4 bytes
		buf.flip();
	}
	@Override
	protected Node<K, V> deserializeNode(final ByteBuffer buf) {
		super.deserializeNode(buf);
		final V factoryV = tree.factoryV();
		//Arrays.fill(values, null);
		for (int i = 0; i < allocated; i++) {
			values[i] = factoryV.deserialize(buf);
		}
		leftid = buf.getInt();
		rightid = buf.getInt();
		return this;
	}

}
