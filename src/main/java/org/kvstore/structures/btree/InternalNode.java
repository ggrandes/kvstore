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
 * Internal Node of BplusTree<K, V>
 * This class is NOT Thread-Safe
 *
 * @param <K> key type (DataHolder<K>)
 * @param <V> value type (DataHolder<V>)
 *
 * @author Guillermo Grandes / guillermo.grandes[at]gmail.com
 */
public final class InternalNode<K extends DataHolder<K>, V extends DataHolder<V>> extends Node<K, V> {
	public final int[] childs;

	protected InternalNode(final BplusTree<K, V> tree) {
		super(tree);
		this.childs = new int[getBOrder()+1];
	}

	@Override
	public int getBOrder() {
		return tree.getBOrderInternal();
	}
	@Override
	public boolean isLeaf() {
		return false;
	}
	@Override
	public boolean isFreeable() {
		return isEmpty() && (childs[0] == NULL_ID);
	}
	@Override
	public void clear() {
		super.clear();
		Arrays.fill(childs, NULL_ID);
	}

	public int getSlotLeft(final int slot) {
		return (slot + 1 > allocated ? slot - 1 : slot); 
	}
	public int getSlotRight(final int slot) {
		return (slot + 1 > allocated ? slot : slot + 1); 
	}


	public boolean add(final K newKey, final int childId) {
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
		return add(slot, newKey, childId);
	}

	public boolean add(final int slot, final K newKey, final int childId) {
		//if (DEBUG) System.out.println("add("+newKey+") i=" + slot);
		if (slot < allocated) { 
			moveElementsRight(keys, slot);
			moveChildsRight(slot+1);
		}
		allocated++;
		keys[slot] = newKey;
		childs[slot+1] = childId;
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
			moveChildsLeft(slot+1);
		}
		if (allocated > 0) allocated--;
		if (DEBUG) System.out.println("[" + id + "] erased up ["+allocated+"] key=" + keys[allocated] + " value="+ childs[allocated+1]);
		keys[allocated] = null;
		childs[allocated+1] = NULL_ID;
		return true;
	}

	@Override
	public InternalNode<K, V> split() { // TODO
		final InternalNode<K, V> newHigh = tree.createInternalNode();
		newHigh.allocId();
		//int j = ((allocated >> 1) | (allocated & 1)); // dividir por dos y sumar el resto (0 o 1)
		int j = (allocated >> 1); // dividir por dos (libro)
		final int newsize = allocated-j;
		//if (DEBUG) System.out.println("split j=" + j);
		System.arraycopy(keys, j, newHigh.keys, 0, newsize);
		System.arraycopy(childs, j+1, newHigh.childs, 0, newsize);
		// TODO: Limpiar la parte alta de los arrays de referencias inutiles
		for (int i = j; i < j + newsize; i++) {
			keys[i] = null;
			childs[i+1] = NULL_ID;
		}
		newHigh.allocated = newsize;
		allocated -= newsize;
		// Ahora habria que hacer un splitShiftKeysLeft para alinear los childs
		tree.putNode(this); // TODO FAKE
		tree.putNode(newHigh); // TODO FAKE
		return newHigh;
	}
	@Override
	public K splitShiftKeysLeft() {
		final K removed = keys[0];
		moveElementsLeft(keys, 0);
		allocated--;
		// TODO: Limpiar la parte alta de los arrays de referencias inutiles
		keys[allocated] = null;
		childs[allocated+1] = NULL_ID;
		return removed;
	}

	// insert child
	protected void moveChildsRight(final int srcPos) {
		//if (DEBUG) System.out.println("moveKeysRight("+srcPos+") allocated=" + allocated + ":" + keys.length + ":" + (allocated-srcPos) + ":" + (keys.length-srcPos-1));
		System.arraycopy(childs, srcPos, childs, srcPos+1, allocated-srcPos+1);
	}
	// remove child
	protected void moveChildsLeft(final int srcPos) {
		//if (DEBUG) System.out.println("moveKeysLeft("+srcPos+") allocated=" + allocated + ":" + keys.length + ":" + (allocated-srcPos-1) + ":" + (keys.length-srcPos-1));
		System.arraycopy(childs, srcPos+1, childs, srcPos, allocated-srcPos);
	}

	public String toString() {
		final StringBuilder sb = new StringBuilder();
		sb.append("[").append("I").append(id).append("]");
		sb.append("(").append(allocated).append("){");
		for (int i = 0; i < allocated; i++) {
			final K k = keys[i];
			if (i == 0) { // left
				sb.append("c").append(childs[i]).append("<");
			}
			else
				sb.append("<");
			sb.append(k);
			sb.append(">c").append(childs[i+1]);
		}
		sb.append("}");
		return sb.toString();
	}

	// ========= Remove Helpers =========

	protected final void merge(final InternalNode<K, V> nodeParent, final int slot, final Node<K, V> nodeFROMx) {
		final InternalNode<K, V> nodeFROM = (InternalNode<K, V>)nodeFROMx;
		final InternalNode<K, V> nodeTO = this;
		final int sizeTO = nodeTO.allocated;
		final int sizeFROM = nodeFROM.allocated;
		// copy keys from nodeFROM to nodeTO
		System.arraycopy(nodeFROM.keys, 0, nodeTO.keys, sizeTO+1, sizeFROM);
		System.arraycopy(nodeFROM.childs, 0, nodeTO.childs, sizeTO+1, sizeFROM+1);
		// add key to nodeTO 
		nodeTO.keys[sizeTO] = nodeParent.keys[slot];
		nodeTO.allocated += sizeFROM + 1; // keys of FROM and key of nodeParent
		// remove key from nodeParent
		nodeParent.remove(slot);
		// Free nodeFROM
		tree.freeNode(nodeFROM);
	}

	protected final void shiftLR(final InternalNode<K, V> nodeParent, final int slot, final Node<K, V> nodeFROMx) {
		final InternalNode<K, V> nodeFROM = (InternalNode<K, V>)nodeFROMx;
		final InternalNode<K, V> nodeTO = this;
		final int sizeTO = nodeTO.allocated;
		final int sizeFROM = nodeFROM.allocated;
		final int shift = ((sizeTO+sizeFROM)/2) - sizeTO;  // num. keys to shift from nodeFROM to nodeTO
		// make space for new keys in nodeTO
		System.arraycopy(nodeTO.keys, 0, nodeTO.keys, shift, sizeTO);
		System.arraycopy(nodeTO.childs, 0, nodeTO.childs, shift, sizeTO+1);
		// move keys and children out of nodeFROM and into nodeTO (and nodeU)
		nodeTO.keys[shift-1] = nodeParent.keys[slot];
		nodeParent.keys[slot] = nodeFROM.keys[sizeFROM-shift];
		System.arraycopy(nodeFROM.keys, sizeFROM-shift+1, nodeTO.keys, 0, shift-1);
		Arrays.fill(nodeFROM.keys, sizeFROM-shift, sizeFROM, null);
		System.arraycopy(nodeFROM.childs, sizeFROM-shift+1, nodeTO.childs, 0, shift);
		Arrays.fill(nodeFROM.childs, sizeFROM-shift+1, sizeFROM+1, Node.NULL_ID);
		nodeTO.allocated += shift;
		nodeFROM.allocated -= shift;
	}

	protected final void shiftRL(final InternalNode<K, V> nodeParent, final int slot, final Node<K, V> nodeFROMx) {
		final InternalNode<K, V> nodeFROM = (InternalNode<K, V>)nodeFROMx;
		final InternalNode<K, V> nodeTO = this;
		final int sizeTO = nodeTO.allocated;
		final int sizeFROM = nodeFROM.allocated;
		final int shift = ((sizeTO+sizeFROM)/2) - sizeTO;  // num. keys to shift from nodeFROM to nodeTO
		// shift keys and children from nodeFROM to nodeTO
		nodeTO.keys[sizeTO] = nodeParent.keys[slot];
		System.arraycopy(nodeFROM.keys, 0, nodeTO.keys, sizeTO+1, shift-1);
		System.arraycopy(nodeFROM.childs, 0, nodeTO.childs, sizeTO+1, shift);
		nodeParent.keys[slot] = nodeFROM.keys[shift-1];
		// delete keys and children from nodeFROM
		System.arraycopy(nodeFROM.keys, shift, nodeFROM.keys, 0, sizeFROM-shift);
		Arrays.fill(nodeFROM.keys, sizeFROM-shift, sizeFROM, null);
		System.arraycopy(nodeFROM.childs, shift, nodeFROM.childs, 0, sizeFROM-shift+1);
		Arrays.fill(nodeFROM.childs, sizeFROM-shift+1, sizeFROM+1, Node.NULL_ID);
		nodeFROM.allocated -= shift;
		nodeTO.allocated += shift;
	}

	/**
	 * Check if underflow occurred in child of slot
	 * @param slot the index of a child in nodeParent (this)
	 */
	protected boolean checkUnderflow(final int slot) {
		if (childs[slot] == NULL_ID) return false;
		if (slot == 0) {
			return checkUnderflowWithRight(slot); // use nodeParent right sibling
		}
		else {
			if (getSlotLeft(slot) == slot) {
				return checkUnderflowWithRight(slot); // use nodeParent right sibling
			}
			else {
				return checkUnderflowWithLeft(slot);  // use nodeParent left sibling
			}
		}
	}

	/**
	 * Check if underflow occurred in child of slot
	 * @param slot the index of a child in nodeParent
	 */
	private final boolean checkUnderflowWithLeft(final int slot) {
		final Node<K, V> nodeRight = tree.getNode(childs[slot]);
		if (nodeRight.isUnderFlow()) {
			final Node<K, V> nodeLeft = tree.getNode(childs[slot-1]);
			if (nodeLeft.canMerge(nodeRight)) {
				nodeLeft.merge(this, slot-1, nodeRight);
			} else {
				nodeRight.shiftLR(this, slot-1, nodeLeft);
			}
			//
			// Update Changed Nodes
			tree.putNode(this);
			tree.putNode(nodeLeft);
			tree.putNode(nodeRight);
			return true;
		}
		return false;
	}
	/**
	 * Check if underflow ocurred in child of slot
	 * @param nodeParent
	 * @param slot the index of a child in nodeParent
	 */
	private final boolean checkUnderflowWithRight(final int slot) {
		final Node<K, V> nodeLeft = tree.getNode(childs[slot]);
		if (nodeLeft.isUnderFlow()) {
			final Node<K, V> nodeRight = tree.getNode(childs[slot+1]);
			if (nodeLeft.canMerge(nodeRight)) {
				nodeLeft.merge(this, slot, nodeRight);
				childs[slot] = nodeLeft.id;
			} else {
				nodeLeft.shiftRL(this, slot, nodeRight);
			}
			//
			// Update Changed Nodes
			tree.putNode(this);
			tree.putNode(nodeRight);
			tree.putNode(nodeLeft);
			return true;
		}
		return false;
	}

	// ========= Serialization =========

	@Override
	public int getStructMaxSize() {
		return super.getStructMaxSize() + (childs.length * 4);
	}
	@Override
	public int getStructEstimateSize(final int b) {
		return super.getStructEstimateSize(b) + ((b+1) * 4);
	}
	@Override
	public void serialize(final ByteBuffer buf) {
		super.serialize(buf);
		for (int i = 0; i < allocated+1; i++) {
			buf.putInt(childs[i]);				// 4 bytes
		}
		buf.flip();
	}
	@Override
	protected Node<K, V> deserializeNode(final ByteBuffer buf) {
		super.deserializeNode(buf);
		for (int i = 0; i < allocated+1; i++) {
			childs[i] = buf.getInt();
		}
		return this;
	}

}
