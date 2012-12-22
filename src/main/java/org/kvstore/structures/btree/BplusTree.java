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

import java.util.Iterator;

import org.kvstore.holders.DataHolder;
import org.kvstore.structures.stack.IntStack;
import org.kvstore.structures.stack.ObjectStack;
import org.kvstore.utils.GenericFactory;

/**
 * <p>
 * Abstract class Implementation of B+Tree.
 * <p>
 * This class is Thread-Safe.
 * 
 * <p>
 * Note that the iterator cannot be guaranteed to be thread-safe as it is,
 * generally speaking, impossible to make any hard guarantees in the presence of
 * unsynchronized concurrent modification.
 * 
 * <p>
 * All TreeEntry pairs returned by methods in this class and its views represent
 * snapshots of mappings at the time they were produced. They do not support the
 * Entry.setValue method.
 *
 * @author Guillermo Grandes / guillermo.grandes[at]gmail.com
 * @references <a href="https://github.com/jankotek/JDBM3">JDBM3</a> / <a href="http://opendatastructures.org/">ODS</a>
 */
public abstract class BplusTree<K extends DataHolder<K>, V extends DataHolder<V>> implements Iterable<BplusTree.TreeEntry<K,V>> {
	public static final boolean DEBUG = false;
	public static final boolean DEBUG2 = false;

	/**
	 * Minimal B-Order allowed for leaf/internal nodes
	 */
	public static final int MIN_B_ORDER = 5;

	/**
	 * maximum number of children of a node (odd/impar number)
	 * <p><i> Note: density of node minimal (b-order/2), average (b-order*2/3)
	 */
	protected int b_order_internal;
	/**
	 * maximum number of values of a node (odd/impar number)
	 * <p><i> Note: density of node minimal (b-order/2), average (b-order*2/3)
	 */
	protected int b_order_leaf;

	/**
	 * default block size (for HDD normally 512 bytes)
	 */
	protected int blockSize = 512;
	/**
	 * Tree is in valid state?
	 */
	protected boolean validState = false;

	/**
	 * META-DATA: nodeId of the root node
	 */
	protected int rootIdx;
	/**
	 * META-DATA: nodeId of the first/lower node
	 */
	protected int lowIdx;
	/**
	 * META-DATA: nodeId of the last/higher node
	 */
	protected int highIdx;
	/**
	 * META-DATA: elements stored in the tree
	 */
	protected int elements = 0;
	/**
	 * META-DATA: height of the tree
	 */
	protected int height = 0;

	// Factorias para creacion de objetos
	protected final LeafNode<K, V> leafNodeFactory;
	protected final InternalNode<K, V> internalNodeFactory;
	private final GenericFactory<K> genericFactoryK;
	private final GenericFactory<V> genericFactoryV;
	private final K factoryK;
	private final V factoryV;


	/**
	 * Trace Internal Nodes (put/remove) 
	 */
	private final ObjectStack<InternalNode<K, V>> stackNodes = new ObjectStack<InternalNode<K, V>>(16);
	/**
	 * Trace slots (put/remove)
	 */
	private final IntStack stackSlots = new IntStack(16, Node.NULL_ID);

	/**
	 * Create B+Tree
	 * @param autoTune if true the tree try to find best b-order for leaf/internal nodes to fit in a block of b_size bytes
	 * @param isMemory if true the tree is in Memory only
	 * @param b_size if autoTune is true is the blockSize, if false is the b-order for leaf/internal nodes
	 * @param typeK the class type of Keys
	 * @param typeV the class type of Values
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 */
	public BplusTree(final boolean autoTune, final boolean isMemory, int b_size, final Class<K> typeK, final Class<V> typeV) throws InstantiationException, IllegalAccessException {
		genericFactoryK = new GenericFactory<K>(typeK); 
		genericFactoryV = new GenericFactory<V>(typeV);
		factoryK = genericFactoryK.newInstance(); 
		factoryV = genericFactoryV.newInstance(); 
		if (autoTune) {
			findOptimalNodeOrder(b_size);
		}
		else {
			if (b_size < MIN_B_ORDER) b_size = MIN_B_ORDER; // minimal b-order
			b_size += 1 - (b_size % 2); // round odd/impar
			b_order_leaf = b_size;
			b_order_internal = b_size;
			if (!isMemory)
				blockSize = roundBlockSize(getMaxStructSize(), blockSize);
		}
		//
		leafNodeFactory = createLeafNode();
		internalNodeFactory = createInternalNode();
		if (DEBUG || true) {
			System.out.println(this.getClass().getName() + "::BplusTree() LeafNode b=" + b_order_leaf + " size=" + (isMemory ? -1 : leafNodeFactory.getStructMaxSize()));
			System.out.println(this.getClass().getName() + "::BplusTree() InternalNode b=" + b_order_internal+ " size=" + (isMemory ? -1 : internalNodeFactory.getStructMaxSize()));
			if (!isMemory)
				System.out.println(this.getClass().getName() + "::BplusTree() FileStorageBlock blocksize=" + blockSize);
		}
		validState = false;
		//
		System.out.println("BplusTree.hashCode()=" + this.hashCode());
	}

	/**
	 * Return the highest nodeid allocated
	 * @return integer 
	 */
	abstract public int getHighestNodeId();
	/**
	 * Alloc a nodeid of type Leaf/Internal
	 * @param isLeaf
	 * @return integer with nodeid
	 */
	abstract protected int allocNode(final boolean isLeaf);
	/**
	 * Get node from the underlying store
	 * @param nodeid int with nodeid
	 * @return Node<K,V>
	 */
	abstract protected Node<K, V> getNode(final int nodeid);
	/**
	 * Put a node in the underlying store
	 * @param node
	 */
	abstract protected void putNode(final Node<K, V> node);
	/**
	 * Free a node from the underlying store
	 * @param node
	 */
	abstract protected void freeNode(final Node<K, V> node);
	/**
	 * This method is invoked after any operation in the 
	 * tree that set/put a node from underlying store
	 */
	abstract protected void releaseNodes();
	/**
	 * Clear the underlying storage and empty the tree
	 * @return boolean true/false if operation end succesfully
	 */
	abstract protected boolean clearStorage();

	/**
	 * Clear the tree
	 */
	public synchronized void clear() {
		if (clearStorage())
			clearStates();
	}

	/**
	 * Reset elements and create new root node
	 */
	protected void clearStates() {
		// Create new Root
		createRootNode();
	}

	/**
	 * Return the number of elements in the tree
	 * @return
	 */
	public synchronized int size() {
		if (!validState) throw new InvalidStateException();
		return elements;
	}

	/**
	 * Returns true if this tree contains no key-value mappings.
	 * @return true if this tree contains no key-value mappings
	 */
	public synchronized boolean isEmpty() {
		if (!validState) throw new InvalidStateException();
		return _isEmpty();
	}

	/**
	 * Returns true if this tree contains no key-value mappings.
	 * @return true if this tree contains no key-value mappings
	 */
	private final boolean _isEmpty() {
		return (elements == 0);
	}

	/**
	 * Create a LeafNode without alloc nodeid for this
	 * @return
	 */
	protected LeafNode<K, V> createLeafNode() {
		return new LeafNode<K, V>(this);
	}
	/**
	 * Create an InternalNode without alloc nodeid for this
	 * @return
	 */
	protected InternalNode<K, V> createInternalNode() {
		return new InternalNode<K, V>(this);
	}
	/**
	 * Create a Leaf root node and alloc nodeid for this
	 */
	protected void createRootNode() {
		final Node<K, V> node = createLeafNode();
		node.allocId();
		rootIdx = node.id;
		height = 1;
		elements = 0;
		putNode(node);
	}

	/**
	 * Get the maximal size for a node
	 * @return integer with the max size of a leaf / internal node
	 */
	private int getMaxStructSize() {
		final int leafSize = (createLeafNode().getStructMaxSize());
		final int internalSize = (createInternalNode().getStructMaxSize());
		return Math.max(leafSize, internalSize);
	}

	/**
	 * Calculate optimal values for b-order to fit in a block of block_size
	 * @param block_size integer with block size (bytes)
	 */
	private void findOptimalNodeOrder(final int block_size) {
		final Node<K, V> leaf = createLeafNode();
		final Node<K, V> internal = createInternalNode();
		// Get Maximal Size
		final int nodeSize = Math.max(leaf.getStructEstimateSize(MIN_B_ORDER), internal.getStructEstimateSize(MIN_B_ORDER));
		// Find minimal blockSize
		blockSize = ((nodeSize > block_size) ? roundBlockSize(nodeSize, block_size) : block_size);
		// Find b-order for Leaf Nodes
		b_order_leaf = findOptimalNodeOrder(leaf);
		// Find b-order for Internal Nodes
		b_order_internal = findOptimalNodeOrder(internal);
		//
	}

	/**
	 * Find b-order for a blockSize of this tree
	 * @param node of type Leaf or Integernal
	 * @return integer with b-order
	 */
	private int findOptimalNodeOrder(final Node<K, V> node) {
		int low = MIN_B_ORDER; // minimal b-order
		int high = (blockSize / node.getStructEstimateSize(1)) << 2; // estimate high b-order

		while (low <= high) {
			int mid = ((low + high) >>> 1); 
			mid += (1 - (mid % 2));
			int nodeSize = node.getStructEstimateSize(mid);

			//if (DEBUG) 
			//System.out.println(this.getClass().getName() + "::findOptimalNodeOrder(" + node.getClass().getName() + ") blockSize=" + blockSize + " nodeSize=" + nodeSize + " b_low=" + low + " b_order=" + mid + " b_high=" + high);

			if (nodeSize < blockSize) {
				low = mid + 2;
			}
			else if (nodeSize > blockSize) {
				high = mid - 2;
			}
			else {
				return mid;
			}
		}
		return low-2;
	}

	/**
	 * Round a size with a blocksize
	 * @param size
	 * @param blocksize
	 * @return
	 */
	private int roundBlockSize(final int size, final int blocksize) {
		return (((size / blocksize) + ((size % blocksize) == 0 ? 0 : 1)) * blocksize);
	}

	/**
	 * @return B-order of a InternalNode 
	 */
	public int getBOrderInternal() {
		return b_order_internal;
	}
	/**
	 * @return B-order of a LeafNode 
	 */
	public int getBOrderLeaf() {
		return b_order_leaf;
	}

	/**
	 * @return GenericFactory to create Keys
	 */
	protected GenericFactory<K> getGenericFactoryK() {
		return genericFactoryK;
	}
	/**
	 * @return GenericFactory to create Values
	 */
	protected GenericFactory<V> getGenericFactoryV() {
		return genericFactoryV;
	}

	/**
	 * @return Return Key
	 */
	protected K factoryK() {
		return factoryK;
	}
	/**
	 * @return Return Value
	 */
	protected V factoryV() {
		return factoryV;
	}

	/**
	 * Find node that can hold the key
	 * @param key
	 * @return LeafNode<K, V> containing the key or null if not found
	 */
	private final LeafNode<K, V> findLeafNode(final K key, final boolean tracePath) {
		Node<K, V> node = getNode(rootIdx);
		if (tracePath) {
			stackNodes.clear();
			stackSlots.clear();
		}
		while (!node.isLeaf()) {
			final InternalNode<K, V> nodeInternal = (InternalNode<K, V>)node;
			int slot = node.findSlotByKey(key);
			slot = ((slot < 0) ? (-slot)-1 : slot+1);
			if (tracePath) {
				stackNodes.push(nodeInternal);
				stackSlots.push(slot);
			}
			node = getNode(nodeInternal.childs[slot]);
			if (node == null) {
				System.out.println("ERROR childs["+slot+"] in node=" + nodeInternal);
				return null;
			}
		}
		return (node.isLeaf() ? (LeafNode<K, V>)node : null);
	}

	/**
	 * Returns the height of this tree.
	 * @return int with height of this tree, 0 is tree is empty
	 */
	public synchronized int getHeight() {
		if (!validState) throw new InvalidStateException();
		if (elements == 0) return 0;
		//		int height = 0;
		//		try {
		//			int nodeIdx = rootIdx;
		//			Node<K, V> nodeFind = getNode(nodeIdx);
		//			while (!nodeFind.isLeaf()) {
		//				++height;
		//				nodeIdx = ((InternalNode<K, V>)nodeFind).childs[0];
		//				nodeFind = getNode(nodeIdx);
		//			}
		//			++height;
		//		} finally {
		//			releaseNodes();
		//		}
		return height;
	}

	/**
	 * Returns the first (lowest) key currently in this tree.
	 * @return key
	 */
	public synchronized K firstKey() {
		if (!validState) throw new InvalidStateException();
		final LeafNode<K, V> node = findSideLeafNode(true);
		if (node == null) return null;
		return node.keys[0];
	}

	/**
	 * Returns the last (highest) key currently in this tree.
	 * @return key
	 */
	public synchronized K lastKey() {
		if (!validState) throw new InvalidStateException();
		final LeafNode<K, V> node = findSideLeafNode(false);
		if (node == null) return null;
		return node.keys[node.allocated-1];
	}

	/**
	 * Returns the first (lowest) key currently in this tree.
	 * @return key
	 */
	public synchronized TreeEntry<K, V> firstEntry() {
		if (!validState) throw new InvalidStateException();
		final LeafNode<K, V> node = findSideLeafNode(true);
		if (node == null) return null;
		final K key = node.keys[0];
		final V value = node.values[0];
		return ((key == null) ? null : new TreeEntry<K, V>(key, value));
	}

	/**
	 * Returns the last (highest) key currently in this tree.
	 * @return key
	 */
	public synchronized TreeEntry<K, V> lastEntry() {
		if (!validState) throw new InvalidStateException();
		final LeafNode<K, V> node = findSideLeafNode(false);
		if (node == null) return null;
		final int slot = node.allocated-1;
		final K key = node.keys[slot];
		final V value = node.values[slot];
		return ((key == null) ? null : new TreeEntry<K, V>(key, value));
	}

	/**
	 * Removes and returns a key-value mapping associated with the least key in this map, or null if the map is empty.
	 * @return entry
	 */
	public synchronized TreeEntry<K, V> pollFirstEntry() {
		final TreeEntry<K, V> entry = firstEntry();
		if (entry != null) remove(entry.getKey());
		return entry;
	}

	/**
	 * Removes and returns a key-value mapping associated with the greatest key in this map, or null if the map is empty.
	 * @return entry
	 */
	public synchronized TreeEntry<K, V> pollLastEntry() {
		final TreeEntry<K, V> entry = lastEntry();
		if (entry != null) remove(entry.getKey());
		return entry;
	}

	/**
	 * Return the first/low or last/high LeafNode in the Tree
	 * @param lowORhigh true first/low node, false last/high node
	 * @return the LeafNoder head/first/lower or tail/last/higher
	 */
	private final LeafNode<K, V> findSideLeafNode(final boolean lowORhigh) {
		if (_isEmpty()) return null;
		try {
			int nodeIdx = (lowORhigh ? lowIdx : highIdx); // rootIdx;
			Node<K, V> nodeFind = getNode((nodeIdx == 0) ? rootIdx : nodeIdx);
			while (!nodeFind.isLeaf()) {
				nodeIdx = ((InternalNode<K, V>)nodeFind).childs[(lowORhigh ? 0 : nodeFind.allocated)];
				nodeFind = getNode(nodeIdx);
			}
			return (nodeFind.isLeaf() ? (LeafNode<K, V>)nodeFind : null);
		} finally {
			releaseNodes();
		}
	}

	/**
	 * Returns the least key greater than or equal to the given key, or null if there is no such key.
	 * @param key the key
	 * @return the least key greater than or equal to key, or null if there is no such key 
	 */
	public synchronized K ceilingKey(final K key) {
		// Retorna la clave mas cercana mayor o igual a la clave indicada
		return getRoundKey(key, true, true);
	}

	/**
	 * Returns the greatest key less than or equal to the given key, or null if there is no such key.
	 * @param key the key
	 * @return the greatest key less than or equal to key, or null if there is no such key
	 */
	public synchronized K floorKey(final K key) {
		// Retorna la clave mas cercana menor o igual a la clave indicada
		return getRoundKey(key, false, true);
	}

	/**
	 * Returns the least key strictly greater than the given key, or null if there is no such key.
	 * @param key the key
	 * @return the least key strictly greater than the given key, or null if there is no such key.
	 */
	public synchronized K higherKey(final K key) {
		// Retorna la clave mas cercana mayor a la clave indicada
		return getRoundKey(key, true, false);
	}

	/**
	 * Returns the greatest key strictly less than the given key, or null if there is no such key.
	 * @param key the key
	 * @return the greatest key strictly less than the given key, or null if there is no such key.
	 */
	public synchronized K lowerKey(final K key) {
		// Retorna la clave mas cercana menor a la clave indicada
		return getRoundKey(key, false, false);
	}

	/**
	 * Return ceilingKey, floorKey, higherKey or lowerKey
	 * @param key the key
	 * @param upORdown true returns ceilingKey, false floorKey
	 * @param acceptEqual true returns equal keys, otherwise, only higher or lower
	 * @return the key found or null if not found
	 */
	private final K getRoundKey(final K key, final boolean upORdown, final boolean acceptEqual) {
		final TreeEntry<K, V> entry = getRoundEntry(key, upORdown, acceptEqual);
		if (entry == null) return null;
		return entry.getKey();
	}	

	/**
	 * Returns the least key greater than or equal to the given key, or null if there is no such key.
	 * @param key the key
	 * @return the Entry with least key greater than or equal to key, or null if there is no such key 
	 */
	public synchronized TreeEntry<K, V> ceilingEntry(final K key) {
		// Retorna la clave mas cercana mayor o igual a la clave indicada
		return getRoundEntry(key, true, true);
	}

	/**
	 * Returns the greatest key less than or equal to the given key, or null if there is no such key.
	 * @param key the key
	 * @return the Entry with greatest key less than or equal to key, or null if there is no such key
	 */
	public synchronized TreeEntry<K, V> floorEntry(final K key) {
		// Retorna la clave mas cercana menor o igual a la clave indicada
		return getRoundEntry(key, false, true);
	}

	/**
	 * Returns the least key strictly greater than the given key, or null if there is no such key.
	 * @param key the key
	 * @return the Entry with least key strictly greater than the given key, or null if there is no such key.
	 */
	public synchronized TreeEntry<K, V> higherEntry(final K key) {
		// Retorna la clave mas cercana mayor a la clave indicada
		return getRoundEntry(key, true, false);
	}

	/**
	 * Returns the greatest key strictly less than the given key, or null if there is no such key.
	 * @param key the key
	 * @return the Entry with greatest key strictly less than the given key, or null if there is no such key.
	 */
	public synchronized TreeEntry<K, V> lowerEntry(final K key) {
		// Retorna la clave mas cercana menor a la clave indicada
		return getRoundEntry(key, false, false);
	}

	/**
	 * Return ceilingEntry, floorEntry, higherEntry or lowerEntry
	 * @param key the key
	 * @param upORdown true returns ceiling/higher, false floor/lower
	 * @param acceptEqual true returns equal keys, otherwise, only higher or lower
	 * @return the Entry found or null if not found
	 */
	private final TreeEntry<K, V> getRoundEntry(final K key, final boolean upORdown, final boolean acceptEqual) {
		if (!validState) throw new InvalidStateException();
		if (_isEmpty()) return null;
		if (key == null) return null;
		try {
			LeafNode<K, V> node = findLeafNode(key, false);
			if (node == null) return null;
			int slot = node.findSlotByKey(key);
			if (upORdown) { 
				// ceiling / higher
				slot = ((slot < 0) ? (-slot)-1 : (acceptEqual ? slot : slot+1));
				if (slot >= node.allocated) {
					node = node.nextNode();
					if (node == null) return null;
					slot = 0;
				}
			}
			else { 
				// floor / lower
				slot = ((slot < 0) ? (-slot)-2 : (acceptEqual ? slot : slot-1));
				if (slot < 0) {
					node = node.prevNode();
					if (node == null) return null;
					slot = node.allocated-1;
				}
			}
			return ((node.keys[slot] == null) ? null : new TreeEntry<K, V>(node.keys[slot], node.values[slot]));
		} finally {
			releaseNodes();
		}
	}

	/**
	 * Find a Key in the Tree
	 * @param key to find
	 * @return Value found or null if not
	 */
	public synchronized V get(final K key) {
		if (!validState) throw new InvalidStateException();
		if (_isEmpty()) return null;
		if (key == null) return null;
		try {
			final LeafNode<K, V> node = findLeafNode(key, false);
			if (node == null) return null;
			int slot = node.findSlotByKey(key);
			if (slot >= 0) {
				return node.values[slot];
			}
			return null;
		} finally {
			releaseNodes();
		}
	}

	/**
	 * Remove a key from key
	 * @param key to delete
	 * @return true if key was removed, false if not
	 */
	public synchronized boolean remove(final K key) {
		if (!validState) throw new InvalidStateException();
		if (key == null) return false;
		try {
			if (DEBUG2) System.out.println("trying remove key=" + key);
			if (removeIterative(key)) { // removeRecursive(key, rootIdx)
				elements--;
				Node<K, V> nodeRoot = getNode(rootIdx);
				if (nodeRoot.isEmpty() && (elements > 0)) { // root has only one child
					// Si sale un ClassCastException es porque hay algun error en la cuenta de elementos
					rootIdx = ((InternalNode<K, V>)nodeRoot).childs[0];
					// Clean old nodeRoot
					freeNode(nodeRoot);
					if (DEBUG) 
						System.out.println("DECREASES TREE HEIGHT (ROOT): elements=" + elements + " oldRoot=" + nodeRoot.id + " newRoot=" + rootIdx);
					height--; // tree height
				}
				else if (nodeRoot.isEmpty() && nodeRoot.isLeaf() && (elements == 0) && (getHighestNodeId() > 4096)) {
					// Hace un reset del arbol para liberar espacio de modo rapido
					if (DEBUG) 
						System.out.println("RESET TREE: elements=" + elements + " leaf=" + nodeRoot.isLeaf() + " empty=" + nodeRoot.isEmpty() + " id=" + nodeRoot.id + " lastNodeId=" + getHighestNodeId() + " nodeRoot=" + nodeRoot);
					clear();
				}
				else if ((elements == 0) && (!nodeRoot.isLeaf() || !nodeRoot.isEmpty())) {
					System.out.println("ERROR in TREE: elements=" + elements + " rootLeaf=" + nodeRoot.isLeaf() + " rootEmpty=" + nodeRoot.isEmpty() + " rootId=" + nodeRoot.id + " nodeRoot=" + nodeRoot);
				}
				return true;
			}
			return false;
		} finally {
			stackNodes.clear();
			stackSlots.clear();
			releaseNodes();
		}
	}

	/**
	 * Remove the Key from the subtree rooted at the node with nodeid (this function is recursive)
	 * 
	 * @param key to delete
	 * @param nodeid of the subtree to remove key from
	 * @return true if key was removed and false otherwise
	 */
	protected boolean removeRecursive(final K key, final int nodeid) {
		if (nodeid == Node.NULL_ID) return false;  // NOT FOUND
		Node<K, V> nodeDelete = getNode(nodeid);
		if (DEBUG2) System.out.println("trying removeRecursive nodeDelete=" + nodeDelete + " key=" + key);
		int slot = nodeDelete.findSlotByKey(key);
		if (nodeDelete.isLeaf()) {
			if (slot < 0) {
				if (DEBUG2) System.out.println("NOT FOUND nodeDelete=" + nodeDelete + " key=" + key);
				return false; // NOT FOUND
			}
			nodeDelete.remove(slot);
			putNode(nodeDelete);
			return true;
		}
		slot = ((slot < 0) ? (-slot)-1 : slot+1);
		final InternalNode<K, V> nodeDeleteInternal = (InternalNode<K, V>) nodeDelete;
		if (removeRecursive(key, nodeDeleteInternal.childs[slot])) {
			nodeDeleteInternal.checkUnderflow(slot);
			return true;
		}
		return false;
	}

	/**
	 * Remove the Key from the tree (this function is iterative)
	 * 
	 * @param key to delete
	 * @return true if key was removed and false otherwise
	 */
	protected boolean removeIterative(final K key) {
		final LeafNode<K, V> nodeLeaf = findLeafNode(key, true);
		//
		// Find in leaf node for key and delete it
		final int slot = nodeLeaf.findSlotByKey(key);
		if (slot >= 0) { // found
			// Remove Key
			nodeLeaf.remove(slot);
			putNode(nodeLeaf);
			// Iterate back over nodes checking underflow
			while (!stackNodes.isEmpty()) {
				final InternalNode<K, V> node = stackNodes.pop();
				final int slotcheck = stackSlots.pop();
				if (!node.checkUnderflow(slotcheck)) {
					return true;
				}
			}
			return true;
		}
		return false;
	}

	/**
	 * Put the value in the tree (if key already exists, update value)
	 * @param key
	 * @param value
	 * @return true if operation is ok, else false
	 */
	public synchronized boolean put(final K key, final V value) {
		if (!validState) throw new InvalidStateException();
		if (key == null) return false;
		try {
			Node<K, V> splitedNode;
			try {
				splitedNode = putIterative(key, value); // putRecursive(key, value, rootIdx);
			}
			catch (DuplicateKeyException e) {
				return false;
			}
			if (splitedNode != null) {   // root was split, make new root
				InternalNode<K, V> nodeRootNew = createInternalNode();
				nodeRootNew.allocId();
				if (DEBUG) 
					System.out.println("INCREASES TREE HEIGHT (ROOT): elements=" + elements + " oldRoot=" + rootIdx + " newRoot=" + nodeRootNew.id);
				final K newkey = splitedNode.splitShiftKeysLeft();
				putNode(splitedNode);
				nodeRootNew.childs[0] = rootIdx;
				nodeRootNew.keys[0] = newkey;
				nodeRootNew.childs[1] = splitedNode.id;
				nodeRootNew.allocated++;
				rootIdx = nodeRootNew.id;
				putNode(nodeRootNew);
				height++; // tree height
			}
			elements++;
			return true;
		} finally {
			stackNodes.clear();
			stackSlots.clear();
			releaseNodes();
		}
	}

	/**
	 * Put the key/value in the subtree rooted for nodeid (this function is recursive)
	 * 
	 * If node is split by this operation then the return value is the Node
	 * that was created when node was split
	 * 
	 * @param key the key to add
	 * @param value the value to add
	 * @param nodeid the nodeid
	 * @return a new node that was created when node was split, or null if u was not split
	 * @throws DuplicateKeyException
	 */
	protected Node<K, V> putRecursive(K key, final V value, final int nodeid) throws DuplicateKeyException {
		final Node<K, V> nodeFind = getNode(nodeid);
		if (nodeFind == null) {
			if (DEBUG) System.out.println(this.getClass().getName() + "::putRecursive getNode("+nodeid+")=null");
		}
		int slot = nodeFind.findSlotByKey(key);
		if (slot >= 0) {
			if (nodeFind.isLeaf()) { // leaf node, just reset it
				final LeafNode<K, V> node = (LeafNode<K, V>)nodeFind;
				node.set(slot, value);
				putNode(node);
				throw new DuplicateKeyException();
			} 
		}
		slot = ((slot < 0) ? (-slot)-1 : slot+1);
		if (nodeFind.isLeaf()) { // leaf node, just add it
			final LeafNode<K, V> node = (LeafNode<K, V>)nodeFind;
			node.add(slot, key, value);
			putNode(node);
		} 
		else {
			final InternalNode<K, V> node = (InternalNode<K, V>)nodeFind;
			final Node<K, V> splitedNode = putRecursive(key, value, node.childs[slot]);
			if (splitedNode != null) {  // child was split, splitedNode is new child
				key = splitedNode.splitShiftKeysLeft();
				putNode(splitedNode);
				node.add(slot, key, splitedNode.id);
				putNode(node);
			}
		}
		return nodeFind.isFull() ? nodeFind.split() : null;
	}

	/**
	 * Put the key/value in the tree (this function is iterative)
	 * 
	 * If root node is split by this operation then the return value is the Node
	 * that was created when root node was split
	 * 
	 * @param key the key to add
	 * @param value the value to add
	 * @return a new node that was created when root node was split, or null if u was not split
	 * @throws DuplicateKeyException
	 */
	protected Node<K, V> putIterative(final K key, final V value) throws DuplicateKeyException {
		final LeafNode<K, V> nodeLeaf = findLeafNode(key, true);
		if (nodeLeaf == null) {
			final StringBuilder sb = new StringBuilder();
			while (!stackNodes.isEmpty()) {
				sb.append("\n").append(stackNodes.pop());
			}
			throw new NullPointerException("findLeafNode("+key+", true)==null:" + sb.toString());
		}
		//
		// Find in leaf node for key
		int slot = nodeLeaf.findSlotByKey(key);
		if (slot >= 0) { // found, update
			nodeLeaf.set(slot, value);
			putNode(nodeLeaf);
			throw new DuplicateKeyException();
		}
		//
		// not found, add
		slot = (-slot)-1;
		Node<K, V> splitedNode = null;
		nodeLeaf.add(slot, key, value);
		putNode(nodeLeaf);
		splitedNode = (nodeLeaf.isFull() ? nodeLeaf.split() : null);
		//
		// Iterate back over nodes checking overflow / splitting
		while (!stackNodes.isEmpty()) {
			final InternalNode<K, V> node = stackNodes.pop();
			slot = stackSlots.pop();
			if (splitedNode != null) {
				// split occurred in previous phase, splitedNode is new child
				final K childKey = splitedNode.splitShiftKeysLeft();
				putNode(splitedNode);
				node.add(slot, childKey, splitedNode.id);
				putNode(node);
			}
			splitedNode = (node.isFull() ? node.split() : null);
		}
		return splitedNode;
	}

	/**
	 * Return a string version of the tree
	 */
	public synchronized String toString() {
		if (!validState) throw new InvalidStateException();
		try {
			return toStringIterative();
		} finally {
			stackNodes.clear();
			stackSlots.clear();
			releaseNodes();
		}
	}

	/**
	 * A iterative algorithm for converting this tree into a string
	 * @return String representing human readable tree
	 */
	private String toStringIterative() {
		final String PADDING = "                                 ";
		final StringBuilder sb = new StringBuilder();
		int elements_debug_local_recounter = 0;
		Node<K, V> node = null;
		int nodeid = rootIdx;
		int depth = 0;
		stackSlots.clear();
		stackSlots.push(rootIdx); // init seed, root node
		boolean lastIsInternal = !Node.isLeaf(rootIdx); 
		while (!stackSlots.isEmpty()) {
			nodeid = stackSlots.pop();
			node = getNode(nodeid);
			if (!node.isLeaf()) {
				for (int i = node.allocated; i >= 0; i--) {
					stackSlots.push(((InternalNode<K, V>)node).childs[i]);
				}
			}
			else {
				elements_debug_local_recounter += node.allocated;
			}
			// For Indentation
			if (lastIsInternal || !node.isLeaf()) { // Last or Curret are Internal
				depth += (lastIsInternal ? +1 : -1);
			}
			lastIsInternal = !node.isLeaf();
			sb.append(PADDING.substring(0, Math.min(PADDING.length(), Math.max(depth-1, 0))));
			//
			sb.append(node.toString()).append("\n");
		}

		// Count elements
		sb
		.append("height=").append(getHeight())
		.append(" root=").append(rootIdx)
		.append(" low=").append(lowIdx)
		.append(" high=").append(highIdx)
		.append(" elements=").append(elements)
		.append(" recounter=").append(elements_debug_local_recounter);

		return sb.toString();
	}

	/**
	 * A recursive algorithm for converting this tree into a string
	 * @return String representing human readable tree
	 */
	@SuppressWarnings("unused")
	private String toStringRecursive() {
		final StringBuilder sb = new StringBuilder();
		int elements_debug_local_recounter = toString(rootIdx, sb, 0);
		sb
		.append("height=").append(getHeight())
		.append(" root=").append(rootIdx)
		.append(" low=").append(lowIdx)
		.append(" high=").append(highIdx)
		.append(" elements=").append(elements)
		.append(" recounter=").append(elements_debug_local_recounter);
		return sb.toString();
	}

	/**
	 * A recursive algorithm for converting this tree into a string
	 * 
	 * @param nodeid the subtree to add to the the string
	 * @param sb a StringBuilder for building the string
	 */
	private int toString(final int nodeid, final StringBuilder sb, final int depth) {
		if (nodeid == Node.NULL_ID) return 0;
		final Node<K, V> node = getNode(nodeid);
		int elements_debug_local_recounter = 0;
		if (node == null) {
			if (DEBUG) System.out.println(this.getClass().getName() + "::toString() getNode("+nodeid+")=null");
			return 0;
		}
		int i = 0;
		sb
		.append("           ".substring(0, depth))
		//.append("[").append(nodeIdx).append("]")
		.append(node.toString()).append("\n");
		if (node.isLeaf()) {
			elements_debug_local_recounter += node.allocated;
		}
		while((i < node.allocated) && (node.keys[i] != null)) {
			if (!node.isLeaf()) {
				elements_debug_local_recounter += toString(((InternalNode<K, V>)node).childs[i], sb, depth+1);
			}
			i++;
		}
		if (!node.isLeaf()) {
			elements_debug_local_recounter += toString(((InternalNode<K, V>)node).childs[i], sb, depth+1);
		}
		return elements_debug_local_recounter;
	}

	// ========== Iterator

	/**
	 * Note that the iterator cannot be guaranteed to be thread-safe as it is,
	 * generally speaking, impossible to make any hard guarantees in the
	 * presence of unsynchronized concurrent modification.
	 * 
	 * @returns iterator over values in map
	 */
	public Iterator<BplusTree.TreeEntry<K, V>> iterator() {
		return new TreeIterator<BplusTree.TreeEntry<K, V>>(this);
	}

	class TreeIterator<T extends BplusTree.TreeEntry<K, V>> implements Iterator<T> {
		final BplusTree<K, V> tree;
		T lastReturned = null;
		T nextElement = null;
		boolean hasBegin = false;

		TreeIterator(final BplusTree<K, V> tree) {
			this.tree = tree;
		}

		@SuppressWarnings("unchecked")
		private final T nextEntry() {
			if (!hasBegin) {
				nextElement = (T)tree.firstEntry();
				return nextElement;
			}
			else {
				if (lastReturned.getKey().compareTo(nextElement.getKey()) >= 0) {
					nextElement = (T)tree.higherEntry(lastReturned.getKey());
				}
			}
			return (nextElement);
		}

		@Override
		public boolean hasNext() {
			return (nextEntry() != null);
		}

		@Override
		public T next() {
			lastReturned = nextEntry();
			hasBegin = true;
			if (nextElement == null)
				throw new java.util.NoSuchElementException();
			return nextElement;
		}

		@Override
		public void remove() {
			if (lastReturned == null)
				throw new IllegalStateException();
			tree.remove(lastReturned.getKey());
		}

	}

	// ========== Exceptions

	/**
	 * Exception throwed then set a key already existent in the tree 
	 */
	static class DuplicateKeyException extends Exception {
		private static final long serialVersionUID = 42L;
	}

	/**
	 * Exception throwed then MetaData is invalid
	 */
	public static class InvalidDataException extends Exception {
		private static final long serialVersionUID = 42L;
		public InvalidDataException(final String str) {
			super(str);
		}
	}
	/**
	 * Exception throwed when Tree is in invalid state (closed) 
	 */
	public static class InvalidStateException extends RuntimeException {
		private static final long serialVersionUID = 42L;
	}

	// ========== TreeEntry

	/**
	 * A map entry (key-value pair). These TreeEntry are read-only objects. 
	 *
	 * @param <K> the key
	 * @param <V> the value
	 */
	public static class TreeEntry<K, V> implements java.util.Map.Entry<K,V> {
		private final K key;
		private final V value;

		private TreeEntry(final K key, final V value) {
			this.key = key;
			this.value = value;
		}

		/**
		 * Returns the key corresponding to this entry. 
		 * @return the key corresponding to this entry 
		 */
		@Override
		public K getKey() {
			return key;
		}

		/**
		 * Returns the value corresponding to this entry.  
		 * @return the value corresponding to this entry
		 */
		@Override
		public V getValue() {
			return value;
		}

		/**
		 * operation not supported.
		 * @param value
		 * @throws UnsupportedOperationException operation is not supported
		 */
		@Override
		public V setValue(final V value) {
			throw new UnsupportedOperationException();
		}

		/**
		 * Compares the specified object with this entry for equality. Returns true if the given object is also a map entry and the two entries represent the same mapping.
		 * @param other object to compare
		 * @return boolean true if equals
		 */
		@Override
		public boolean equals(final Object other) {
			if (other == null) return false;
			if (!(other instanceof TreeEntry)) return false;
			//
			final TreeEntry<K, V> e1 = this;
			@SuppressWarnings("unchecked")
			final TreeEntry<K, V> e2 = (TreeEntry<K, V>) other;
			//
			final boolean keyEquals = ((e1.getKey()==null) ? (e2.getKey()==null) : e1.getKey().equals(e2.getKey()));
			final boolean valueEquals = ((e1.getValue()==null) ? (e2.getValue()==null) : e1.getValue().equals(e2.getValue()));
			///
			return (keyEquals && valueEquals);
		}

		/**
		 * Returns a string representation of the object.
		 */
		@Override
		public String toString() {
			return (key == null ? "null" : key.toString()) + "=" + (value == null? "null" : value.toString());
		}
	}
}
