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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.kvstore.holders.DataHolder;
import org.kvstore.io.FileBlockStore;
import org.kvstore.io.FileBlockStore.CallbackSync;
import org.kvstore.io.FileStreamStore;
import org.kvstore.pool.BufferStacker;
import org.kvstore.structures.bitset.SimpleBitSet;
import org.kvstore.structures.hash.IntHashMap;
import org.kvstore.structures.hash.IntLinkedHashMap;

/**
 * Implementation of B+Tree in File
 * This class is Thread-Safe
 *
 * @author Guillermo Grandes / guillermo.grandes[at]gmail.com
 */
public final class BplusTreeFile<K extends DataHolder<K>, V extends DataHolder<V>> extends BplusTree<K, V> {
	/**
	 * Disable all Caches
	 */
	private final boolean disableAllCaches = false;

	/**
	 * Full erase of block when freed
	 */
	private final boolean cleanBlocksOnFree = false;
	/**
	 * Enable IO Stats (performance loss)
	 */
	private final boolean enableIOStats = false;
	/**
	 * Enable dirty blocks check for debugging only
	 */
	private final boolean enableDirtyCheck = false;

	/**
	 * MAGIC_1 in begining of metadata
	 */
	private static final int MAGIC_1 = 0x42D6AECB;
	/**
	 * MAGIC_1 in ending of metadata
	 */
	private static final int MAGIC_2 = 0x6B708B42;
	/**
	 * File for storage
	 */
	private final File fileStorage;
	/**
	 * File for free blocks
	 */
	private final File fileFreeBlocks;
	/**
	 * File for redo
	 */
	private final File fileRedo;
	/**
	 * Storage Object
	 */
	private final FileBlockStore storage;
	/**
	 * Read cache (elements) for Leaf Nodes
	 */
	private int readCacheLeaf = 128;
	/**
	 * Read cache (elements) for Internal Nodes
	 */
	private int readCacheInternal = 128;

	/**
	 * Bitset with id of free blocks to reuse
	 */
	private SimpleBitSet freeBlocks;		// META-DATA: bitset of free blocks in storage
	/**
	 * Current blockid/nodeid from underlying storage
	 */
	private int storageBlock = 0;			// META-DATA: id of last blockid for nodes
	/**
	 * Bitset with id of dirty blocks pending to commit
	 */
	private SimpleBitSet dirtyCheck = new SimpleBitSet();
	/**
	 * Redo Storage
	 */
	private final FileStreamStore redoStore;
	/**
	 * Use Redo?
	 */
	private boolean useRedo = true;
	/**
	 * Use Dedicated Thread for Redo?
	 */
	private boolean useRedoThread = false;

	/**
	 * Redo Queue for Dedicated Thread
	 */
	final ArrayBlockingQueue<ByteBuffer> redoQueue = new ArrayBlockingQueue<ByteBuffer>(0x1);
	/**
	 * Redo Thread
	 */
	private Thread redoThread = null;
	private AtomicBoolean doShutdownRedoThread = new AtomicBoolean();

	// Remember parameters for recovery
	private final transient boolean autoTune;
	private final transient int b_size;
	private final transient String fileName;

	/**
	 * Create B+Tree in File
	 * 
	 * @param autoTune if true the tree try to find best b-order for leaf/internal nodes to fit in a block of b_size bytes
	 * @param b_size if autoTune is true is the blockSize, if false is the b-order for leaf/internal nodes
	 * @param typeK the class type of Keys
	 * @param typeV the class type of Values
	 * @param fileName base file name (example: /tmp/test)
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 */
	public BplusTreeFile(final boolean autoTune, int b_size, final Class<K> typeK, final Class<V> typeV, final String fileName) throws InstantiationException, IllegalAccessException {
		super(autoTune, false, b_size, typeK, typeV);
		//
		this.autoTune = autoTune;
		this.b_size = b_size;
		this.fileName = fileName;
		//
		createReadCaches();
		//
		fileStorage = new File(fileName + ".data");
		fileFreeBlocks = new File(fileName + ".free");
		fileRedo = new File(fileName + ".redo");
		//
		bufstack = BufferStacker.getInstance(blockSize, isDirect);
		freeBlocks = new SimpleBitSet();
		storage = new FileBlockStore(fileStorage.getAbsolutePath(), blockSize, isDirect);
		redoStore = new FileStreamStore(fileRedo.getAbsolutePath(), blockSize<<1);
		redoStore.setSyncOnFlush(false);
		redoStore.setFlushOnWrite(true);
		validState = false;
	}

	@Override
	protected boolean clearStorage() {
		// Reset Storage
		storage.delete();
		redoStore.delete();
		return (storage.open() && redoStore.open());
	}

	@Override
	protected void clearStates() {
		// Clear Caches
		clearReadCaches();
		clearWriteCaches();
		// Reset Counters
		maxInternalNodes = 0;
		maxLeafNodes = 0;
		storageBlock = 0;
		// Reset bitsets
		freeBlocks = new SimpleBitSet();
		dirtyCheck = new SimpleBitSet();
		//
		// Reset Root node
		super.clearStates();
		// Sync changes
		validState = writeMetaData(false);
		sync();
	}

	// ===================================== Node management

	@Override
	public int getHighestNodeId() {
		return storageBlock;
	}

	@Override
	protected int allocNode(final boolean isLeaf) {
		int id = freeBlocks.nextSetBit(0);
		if (id < 0) {
			if (isLeaf) {
				maxLeafNodes++;
			}
			else {
				maxInternalNodes++;
			}
			id = ++storageBlock;
		}
		else {
			freeBlocks.clear(id);
		}
		return (isLeaf ? id : -id);
	}

	@Override
	protected void freeNode(final Node<K, V> node) {
		final int nodeid = node.id;
		if (nodeid == Node.NULL_ID) {
			System.out.println(this.getClass().getName() + "::freeNode(" + nodeid + ") ERROR");
			return;
		}
		// Se marca como borrado
		node.delete(); 
		putNode(node);
	}

	@Override
	protected Node<K, V> getNode(final int nodeid) {
		//System.out.println("getNode(" +nodeid+ ")");
		if (nodeid == Node.NULL_ID) {
			System.out.println(this.getClass().getName() + "::getNode(" + nodeid + ") ERROR");
			return null;
		}
		if (disableAllCaches) {
			return getNodeFromStore(nodeid);
		}
		return getNodeCache(nodeid);
	}

	/**
	 * Get node from file
	 * @param nodeid int with nodeid
	 * @return Node<K,V>
	 */
	@SuppressWarnings("unchecked")
	private Node<K, V> getNodeFromStore(final int nodeid) {
		final int index = nodeid < 0 ? -nodeid : nodeid;
		final ByteBuffer buf = storage.get(index);
		final Node<K, V> node = Node.deserialize(buf, this);
		if (rootIdx == node.id) {
			System.out.println(this.getClass().getName() + "::getNodeFromStore(" + nodeid + ") WARN ROOT NODE READED");
		}
		bufstack.push(buf);
		if (enableIOStats) getIOStat(nodeid).incPhysRead();
		return node;
	}

	@Override
	protected void putNode(final Node<K, V> node) {
		if (disableAllCaches) {
			putNodeToStore(node);
			return;
		}
		setNodeDirty(node);
	}
	/**
	 * Put a node in file
	 * @param node
	 */
	private void putNodeToStore(final Node<K, V> node) {
		final int nodeid = node.id;
		final int index = (nodeid < 0 ? -nodeid : nodeid);
		final ByteBuffer buf = bufstack.pop();
		if (node.isDeleted()) { 	// This block is for delete
			if (cleanBlocksOnFree) {
				buf.clear();
				int cx = (blockSize >> 3); // division by 8
				while (cx-- > 0) {
					buf.putLong(0);	// Fill with zeroes
				}
				buf.flip();
			}
			else {
				node.clean(buf); 	// Generate zeroed minimal buffer
			}
			freeBlocks.set(index); 	// Mark block as free
		}
		else {
			node.serialize(buf);
		}
		storage.set(index, buf);
		if (enableDirtyCheck) dirtyCheck.clear(index);
		if (enableIOStats) getIOStat(nodeid).incPhysWrite();
	}

	@Override
	protected void releaseNodes() {
		final int maxTotalNodes = maxCacheSizeInBytes / blockSize;
		final long ts = System.currentTimeMillis();
		//
		// Calculate how mem is in use
		final int initialDirtyNodesInMem = dirtyLeafNodes.size() + dirtyInternalNodes.size();
		final int initialCacheNodesInMem = cacheLeafNodes.size() + cacheInternalNodes.size();
		final int initialTotalNodesInMem = initialDirtyNodesInMem + initialCacheNodesInMem;
		final boolean doClean = (initialTotalNodesInMem >= maxTotalNodes);
		if (!doClean) return; 
		//
		// Commit excess write-buffers
		final boolean autoSync = (initialDirtyNodesInMem >= (maxTotalNodes/10)); // 10% of nodes are dirty
		if (autoSync) {
			privateSync(true);
		}
		//
		// Discard excess read-buffers
		final int currentCacheNodesInMem = cacheLeafNodes.size() + cacheInternalNodes.size();
		final boolean autoEvict = (currentCacheNodesInMem >= maxTotalNodes); // after clean dirty read-cache are full 
		final int evictedCacheInternal = (autoEvict ? removeEldestElementsFromCache(cacheInternalNodes, readCacheInternal) : 0);
		final int evictedCacheLeaf = (autoEvict ? removeEldestElementsFromCache(cacheLeafNodes, readCacheLeaf) : 0);
		//
		// Show stats
		if (autoSync) {
			final int evictedTotal = evictedCacheInternal+evictedCacheLeaf;
			final int currentUsedMem = (dirtyLeafNodes.size() + dirtyInternalNodes.size() + cacheLeafNodes.size() + cacheInternalNodes.size()) * blockSize / 1024;
			final StringBuilder sb = new StringBuilder();
			sb
			.append("releaseNodes()")
			.append(" maxNodes=").append(maxLeafNodes).append("L/").append(maxInternalNodes).append("I")
			.append(" autoSync=").append(autoSync)
			.append(" dirtys=").append(initialDirtyNodesInMem)
			.append(" caches=").append(initialCacheNodesInMem)
			.append(" evicted=").append(evictedTotal)
			.append(" initialMem=").append(initialTotalNodesInMem * blockSize / 1024).append("KB")
			.append(" currentMem=").append(currentUsedMem).append("KB")
			.append(" ts=").append(System.currentTimeMillis() - ts);
			if (enableDirtyCheck) {
				sb.append(" dirtyBlocks=").append(dirtyCheck.toString());
			}
			System.out.println(sb.toString());
		}
	}

	/**
	 * Evict from Read Cache excess nodes  
	 * @param hash cache to purge
	 * @param maxSize max elements to hold in cache
	 * @return int number of elements evicted
	 */
	@SuppressWarnings("rawtypes")
	private final int removeEldestElementsFromCache(final IntLinkedHashMap<Node> hash, final int maxSize) {
		final int evict = hash.size() - maxSize;
		if (evict <= 0) return 0;

		for (int count = 0; count < evict; count++) {
			hash.removeEldest(); 
		}
		return evict;
	}

	// ===================================== Meta data Managemenet

	/**
	 * Write metadata to file 
	 * @param isClean mark file with clean/true or unclean/false
	 * @return boolean if write is ok
	 */
	private boolean writeMetaData(final boolean isClean) {
		final ByteBuffer buf = bufstack.pop();
		boolean isOK = false;
		buf.putInt(MAGIC_1);
		buf.putInt(blockSize);
		buf.putInt(b_order_leaf);
		buf.putInt(b_order_internal);
		buf.putInt(storageBlock);
		buf.putInt(rootIdx);
		buf.putInt(lowIdx);
		buf.putInt(highIdx);
		buf.putInt(elements);
		buf.putInt(height);
		buf.putInt(maxInternalNodes);
		buf.putInt(maxLeafNodes);
		buf.put((byte) (isClean ? 0xEA : 0x00));
		buf.putInt(MAGIC_2);
		buf.flip();
		isOK = storage.set(0, buf);
		if (isClean)
			storage.sync();
		try {
			if (isClean) {
				SimpleBitSet.serializeToFile(fileFreeBlocks, freeBlocks);
			}
			else {
				fileFreeBlocks.delete();
			}
		}
		catch(IOException e) {
			e.printStackTrace(System.out);
		}
		if (DEBUG) System.out.println(this.getClass().getName() + "::writeMetaData() elements=" + elements + " rootIdx=" + rootIdx + " lastNodeId=" + storageBlock + " freeBlocks=" + freeBlocks.cardinality());
		return isOK;
	}

	/**
	 * Read metadata from file
	 * @return true if file is clean or not
	 * @throws InvalidDataException if metadata is invalid
	 */
	private boolean readMetaData() throws InvalidDataException {
		final ByteBuffer buf = storage.get(0);
		int magic1, magic2, t_b_order_leaf, t_b_order_internal, t_blockSize; // sanity
		boolean isClean = false;
		magic1 = buf.getInt();
		if (magic1 != MAGIC_1) throw new InvalidDataException("Invalid metadata (MAGIC1)");
		t_blockSize = buf.getInt();
		if (t_blockSize != blockSize) throw new InvalidDataException("Invalid metadata (blockSize) " + t_blockSize + " != " + blockSize);		
		t_b_order_leaf = buf.getInt();
		t_b_order_internal = buf.getInt();
		if (t_b_order_leaf != b_order_leaf) throw new InvalidDataException("Invalid metadata (b-order leaf) " + t_b_order_leaf + " != " + b_order_leaf);
		if (t_b_order_internal != b_order_internal) throw new InvalidDataException("Invalid metadata (b-order internal) " + t_b_order_internal + " != " + b_order_internal);
		storageBlock = buf.getInt();
		rootIdx = buf.getInt();
		lowIdx = buf.getInt();
		highIdx = buf.getInt();
		elements = buf.getInt();
		height = buf.getInt();
		maxInternalNodes = buf.getInt();
		maxLeafNodes = buf.getInt();
		isClean = ((buf.get() == ((byte)0xEA)) ? true : false);
		magic2 = buf.getInt();
		if (magic2 != MAGIC_2) throw new InvalidDataException("Invalid metadata (MAGIC2)");
		if (DEBUG) System.out.println(this.getClass().getName() + "::readMetaData() elements=" + elements + " rootIdx=" + rootIdx);
		bufstack.push(buf);
		// Clear Caches
		clearReadCaches();
		clearWriteCaches();
		if (isClean && fileFreeBlocks.exists()) {
			try {
				final SimpleBitSet newFreeBlocks = SimpleBitSet.deserializeFromFile(fileFreeBlocks);
				freeBlocks = newFreeBlocks;
			}
			catch(IOException e) {
				e.printStackTrace(System.out);
			}
		}
		return isClean;
	}

	// ===================================== Store Managemenet

	/**
	 * Create/Clear file
	 */
	public synchronized void create() {
		clear();
	}

	/**
	 * Recovery
	 * @return boolean if all right
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 */
	public synchronized boolean recovery() throws InstantiationException, IllegalAccessException {
		if (storage.isOpen() || redoStore.isOpen()) {
			throw new InvalidStateException();
		}
		if (!storage.open() || !redoStore.open()) {
			storage.close();
			redoStore.close();
			return false;
		}
		//
		final BplusTreeFile<K, V> treeTmp = new BplusTreeFile<K, V>(autoTune, b_size, getGenericFactoryK().type, getGenericFactoryV().type, fileName + ".recover");
		final K factoryK = factoryK();
		final V factoryV = factoryV();
		final int blocks = storage.sizeInBlocks();
		//
		treeTmp.clear();
		treeTmp.setUseRedo(false);
		//
		// Reconstruct Tree, Scan BlockStore for data
		System.out.println("Trying to Recover Blocks: " + blocks);
		for (int index = 1; index < blocks; index++) {
			try {
				if ((index % 100) == 0)
					System.out.println("Recovering block [" + index + "/" + blocks + "]");
				final Node<K, V> node = getNodeFromStore(index); // read
				if (node.isLeaf()) {
					final LeafNode<K, V> leafNode = (LeafNode<K, V>) node;
					for (int i = 0; i < node.allocated; i++) {
						final K key = leafNode.keys[i];
						final V value = leafNode.values[i];
						treeTmp.put(key, value);
					}
				}
			}
			catch (Node.InvalidNodeID e) {
				// Skip
			}
		}
		//
		// Apply Redo Store
		final ByteBuffer buf = bufstack.pop();
		final long size = redoStore.size();
		long offset = 0;
		int count = 1;
		while (offset < size) {
			buf.clear();
			if ((offset = redoStore.read(offset, buf)) < 0) {
				break;
			}
			if ((count++ % 100) == 0)
				System.out.println("Applying Redo [offset=" + offset + "]");
			final int op = buf.get();
			final K key = factoryK.deserialize(buf);
			switch (op) {
			case 0xA: // PUT
				final V value = factoryV.deserialize(buf);
				treeTmp.put(key, value);
				break;
			case 0xB: // REMOVE
				treeTmp.remove(key);
				break;
			}
		}
		bufstack.push(buf);
		treeTmp.close();
		storage.close();
		redoStore.close();
		// Rename data files
		final String ts = getTimeStamp();
		if (renameFileToBroken(fileStorage, ts) &&
				renameFileToBroken(fileRedo, ts)) {
			// Rename tmp to good
			treeTmp.fileStorage.renameTo(fileStorage);
			// Remove tmp redo
			treeTmp.fileRedo.delete();
			// Remove tmp free blocks
			treeTmp.fileFreeBlocks.delete();
			// Remove broken free blocks
			fileFreeBlocks.delete();
			return true;
		}
		return false;
	}

	/**
	 * Open file
	 * @return boolean if all right
	 * @throws InvalidDataException if metadata is invalid
	 */
	public synchronized boolean open() throws InvalidDataException {
		boolean allRight = false;
		boolean isNew = false;
		if (storage.isOpen() || redoStore.isOpen()) {
			throw new InvalidStateException();
		}
		storage.open();
		redoStore.open();
		try {
			if (storage.sizeInBlocks() == 0) {
				clearStates();
				isNew = true;
			}
			try {
				boolean isClean = readMetaData();
				//System.out.println(this.hashCode() + "::open() clean=" + isClean);
				if (isClean && !isNew) {
					if (writeMetaData(false)) {
						populateCache();
						allRight = true;
					}
				}
				else if (!isNew) {
					// Broken
					throw new InvalidDataException("NEED RECOVERY");
				}
			}
			catch (InvalidDataException e) {
				validState = false;
				storage.close();
				redoStore.close();
				throw e;
			}
		} finally {
			releaseNodes();
		}
		validState = true;
		return allRight;
	}

	private static String getTimeStamp() {
		final SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd.HHmmss");
		return sdf.format(new Date());
	}

	private static boolean renameFileToBroken(final File orig, final String ts) {
		return orig.renameTo(new File(orig.getAbsolutePath() + ".broken." + ts));
	}

	/**
	 * Close storage file and sync pending changes/dirty nodes
	 */
	public synchronized void close() {
		if (storage.isOpen()) {
			if (useRedoThread && (redoThread != null)) {
				stopRedoThread(redoThread);
			}
			sync();
			writeMetaData(true);
			if (enableDirtyCheck) System.out.println("dirtyCheck=" + dirtyCheck);
		}
		storage.close();
		redoStore.close();
		clearReadCaches();
		clearWriteCaches();
		validState = false;
		//System.out.println(this.hashCode() + "::close() done");
	}

	/**
	 * Use Redo system?
	 * @param useRedo (default true)
	 */
	public synchronized void setUseRedo(final boolean useRedo) {
		if (this.useRedo && !useRedo) { // Remove Redo
			redoQueue.clear();
			redoStore.clear();
		}
		this.useRedo = useRedo;
	}

	/**
	 * Use Dedicated Thread for Redo?
	 * @param useRedoThread (default false)
	 */
	public synchronized void setUseRedoThread(final boolean useRedoThread) {
		if (this.useRedoThread && !useRedoThread) { // Stop Redo Thread
			if (redoThread != null)
				stopRedoThread(redoThread);
		}
		this.useRedoThread = useRedoThread;
	}

	private void createRedoThread() {
		if (useRedoThread && (redoThread == null)) {
			redoThread = new Thread(new Runnable() {
				@Override
				public void run() {
					try  {
						doShutdownRedoThread.set(false);
						ByteBuffer buf = null; 
						while (!doShutdownRedoThread.get()) {
							buf = redoQueue.poll(1000, TimeUnit.MILLISECONDS);
							if (buf != null)
								redoStore.write(buf);
						}
						endProcess();
					}
					catch (InterruptedException ie) {
						endProcess();
						Thread.currentThread().interrupt(); // Preserve
					}
					catch (Exception e) {
						e.printStackTrace(System.out);
					}
					finally {
						redoThread = null;
					}
				}
				private final void endProcess() {
					if (!redoQueue.isEmpty()) {
						ByteBuffer buf = null;
						while ((buf = redoQueue.poll()) != null) {
							redoStore.write(buf);
						}
					}
					redoStore.sync();
				}
			});
			redoThread.start();
		}

	}

	private void stopRedoThread(final Thread localRedoThread) {
		try {
			doShutdownRedoThread.set(true);
			localRedoThread.join(3000);
			localRedoThread.interrupt();
			localRedoThread.join(30000);
		} catch (Exception e) {
			e.printStackTrace(System.out);
		}
	}

	/**
	 * submit put to redo
	 * @param key
	 * @param value
	 */
	@Override
	protected void submitRedoPut(final K key, final V value) {
		if (!useRedo) return;
		createRedoThread();
		final ByteBuffer buf = (useRedoThread ? ByteBuffer.allocate(1+key.byteLength()+value.byteLength()) : bufstack.pop());
		buf.put((byte) 0xA); // PUT
		key.serialize(buf);
		value.serialize(buf);
		buf.flip();
		if (useRedoThread) {
			try {
				redoQueue.put(buf);
			} catch (InterruptedException e) {
				e.printStackTrace(System.out);
			}
		} else {
			redoStore.write(buf);
			bufstack.push(buf);
		}
	}

	/**
	 * submit remove to redo
	 * @param key
	 */
	@Override
	protected void submitRedoRemove(final K key) {
		if (!useRedo) return;
		createRedoThread();
		final ByteBuffer buf = (useRedoThread ? ByteBuffer.allocate(1+key.byteLength()) : bufstack.pop());
		buf.put((byte) 0xB); // REMOVE
		key.serialize(buf);
		buf.flip();
		if (useRedoThread) {
			try {
				redoQueue.put(buf);
			} catch (InterruptedException e) {
				e.printStackTrace(System.out);
			}
		} else {
			redoStore.write(buf);
			bufstack.push(buf);
		}
	}

	/**
	 * Dump tree in text form to a file
	 * @param file
	 * @throws FileNotFoundException
	 */
	public void dumpStorage(final String file) throws FileNotFoundException {
		PrintStream out = null;
		try {
			out = new PrintStream(new FileOutputStream(file));
			dumpStorage(out);
		}
		finally {
			try { out.close(); } catch(Exception ign) {}
		}
	}

	/**
	 * Dump tree in text form to PrintStream (System.out?)
	 * @param out PrintStream
	 */
	public synchronized void dumpStorage(final PrintStream out) {
		if (!validState) throw new InvalidStateException();
		try {
			final StringBuilder sb = new StringBuilder(4096);
			sb
			.append("#")
			.append("ID").append("\t")
			.append("Node").append("\n");
			for (int i = 1; i < storageBlock; i++) {
				final Node<K, V> node = getNode(i);
				sb
				.append(i).append((rootIdx == i) ? "R\t" : "\t")
				.append(node).append("\n");
				if ((i % 1000) == 0) {
					out.print(sb.toString());
					sb.setLength(0);
				}
			}
			if (sb.length() > 0) {
				out.print(sb.toString());
				sb.setLength(0);
			}
		} finally {
			releaseNodes();
		}
	}

	// ===================================== READ CACHE

	/**
	 * Default cache size in bytes
	 */
	private static final int DEFAULT_CACHE_SIZE_BYTES = 16 * 1024 * 1024;

	/**
	 * Max size of cache in bytes
	 */
	private int maxCacheSizeInBytes = DEFAULT_CACHE_SIZE_BYTES;

	/**
	 * Read cache for internal nodes 
	 */
	@SuppressWarnings("rawtypes")
	private IntLinkedHashMap<Node> cacheInternalNodes;

	/**
	 * Read cache for leaf nodes 
	 */
	@SuppressWarnings("rawtypes")
	private IntLinkedHashMap<Node> cacheLeafNodes;

	/**
	 * Clear caches and Set new value of maximal bytes used for nodes in cache.
	 * 
	 * @param newsize size of cache in bytes (0 only clear caches)
	 * <p>
	 * <ul>
	 * <li> Calculo aproximado del numero de elementos que se pueden mantener en memoria:
	 * <br/> elementos=nodos*(b-leaf-order*2/3)
	 * <li> Calculo aproximado del numero de nodos necesarios para mantener en memoria N elementos:
	 * <br/> nodos=elementos/(b-leaf-order*2/3)
	 * <li> El cache necesario para almacenar N nodos:
	 * <br/> cache-size=nodos*blocksize
	 * </ul>
	 * 
	 * <p> Ejemplos, para almacenar 5millones de registros (bloque de 1k):
	 * 
	 * <ul>
	 * <li> LongHolder (8 bytes) b-leaf-order=63
	 * <br/> 5000000/(63*2/3) = 119047nodos * 1024bytes = 121.904.128 bytes
	 * <li> IntHolder (4 bytes) b-leaf-order=127
	 * <br/> 5000000/(127*2/3) = 59055nodos * 1024bytes = 60.472.320 bytes
	 * </ul>
	 */	
	public synchronized void setMaxCacheSizeInBytes(final int newsize) {
		if (validState) {
			System.out.println(this.getClass().getName() + "::setMaxCacheSizeInBytes newsize=" + newsize + " flushing write-cache");
			privateSync(true);
			clearReadCaches();
		}
		if (newsize >= 1024) { // 1KB minimal
			maxCacheSizeInBytes = newsize;
			createReadCaches();
		}
	}
	/**
	 * @return current value of cache in bytes
	 */
	public synchronized int getMaxCacheSizeInBytes() {
		return maxCacheSizeInBytes;
	}

	/**
	 * Recalculate size of read caches
	 */
	private void recalculateSizeReadCaches() {
		final int maxCacheNodes = (maxCacheSizeInBytes / blockSize);
		readCacheInternal = Math.max((int)(maxCacheNodes * .05f), 37);
		readCacheLeaf = Math.max((int)(maxCacheNodes * .95f), 37);
	}

	/**
	 * Create read caches
	 */
	private void createReadCaches() {
		recalculateSizeReadCaches();
		//if (DEBUG) 
		System.out.println(this.getClass().getName() + "::createReadCaches readCacheInternal=" + readCacheInternal + " readCacheLeaf=" + readCacheLeaf);
		cacheInternalNodes = createCacheLRUlinked(readCacheInternal);
		cacheLeafNodes = createCacheLRUlinked(readCacheLeaf);
	}

	/**
	 * Clear read caches
	 */
	private final void clearReadCaches() {
		// Clear without shrink
		cacheInternalNodes.clear(false);
		cacheLeafNodes.clear(false);
	}

	/**
	 * Create a LRU hashmap of size maxSize 
	 * @param maxSize
	 * @return IntLinkedHashMap
	 */
	@SuppressWarnings("rawtypes")
	private IntLinkedHashMap<Node> createCacheLRUlinked(final int maxSize) {
		return new IntLinkedHashMap<Node>((int)(maxSize * 1.5f), Node.class, true);
	}

	/**
	 * Populate read cache if cache is enabled
	 */
	private void populateCache() {
		if (disableAllCaches) return;
		// Populate Cache
		final long ts = System.currentTimeMillis();
		for (int index = 1; ((index < storageBlock) && (cacheInternalNodes.size() < readCacheInternal) && (cacheLeafNodes.size() < readCacheLeaf)); index++) {
			if (freeBlocks.get(index)) continue; // skip free
			try {
				final Node<K, V> node = getNodeFromStore(index); // read
				(node.isLeaf() ? cacheLeafNodes : cacheInternalNodes).put(node.id, node);
			}
			catch (Node.InvalidNodeID e) {
				freeBlocks.set(index); // mark index as free
			}
		}
		System.out.println("Populated read cache ts=" + (System.currentTimeMillis() - ts) + " blocks=" + storageBlock + " elements=" + elements);
	}

	/**
	 * Get node from cache
	 * @param nodeid int with nodeid
	 * @return Node<K,V>
	 */
	@SuppressWarnings("unchecked")
	private Node<K, V> getNodeCache(final int nodeid) {
		final boolean isLeaf = Node.isLeaf(nodeid);
		boolean responseFromCache = true;
		Node<K, V> node = (isLeaf ? dirtyLeafNodes : dirtyInternalNodes).get(nodeid);
		if (node == null) {
			node = (isLeaf ? cacheLeafNodes : cacheInternalNodes).get(nodeid);
			if (node == null) {
				if (DEBUG2) System.out.println("diskread node id=" + nodeid);
				node = getNodeFromStore(nodeid);
				responseFromCache = false;
				(node.isLeaf() ? cacheLeafNodes : cacheInternalNodes).put(nodeid, node);
			}
		}
		if (enableIOStats) {
			if (responseFromCache) {
				getIOStat(nodeid).incCacheRead();
			}
		}
		return node;
	}

	// ===================================== WRITE CACHE

	/**
	 * Write cache for internal dirty nodes 
	 */
	@SuppressWarnings("rawtypes")
	private final IntHashMap<Node> dirtyInternalNodes = new IntHashMap<Node>(1024, Node.class);

	/**
	 * Write cache for leaf dirty nodes 
	 */
	@SuppressWarnings("rawtypes")
	private final IntHashMap<Node> dirtyLeafNodes = new IntHashMap<Node>(1024, Node.class);

	/**
	 * Comparator for write cache / dirty nodes by nodeid
	 */
	private Comparator<Node<K, V>> dirtyComparatorByID = new Comparator<Node<K, V>>() {
		@Override
		public int compare(final Node<K, V> o1, final Node<K, V> o2) {
			if (o1 == null) {
				if (o2 == null) return 0; // o1 == null & o2 == null
				return 1; // o1 == null & o2 != null
			}
			if (o2 == null) return -1; // o1 != null & o2 == null
			final int thisVal = (o1.id < 0 ? -o1.id : o1.id);
			final int anotherVal = (o2.id < 0 ? -o2.id : o2.id);
			return ((thisVal<anotherVal) ? -1 : ((thisVal==anotherVal) ? 0 : 1));
		}
	};

	/**
	 * Put a node in dirty cache
	 * @param node
	 */
	private void setNodeDirty(final Node<K, V> node) {
		final int nodeid = node.id;
		final int index = nodeid < 0 ? -nodeid : nodeid;
		(node.isLeaf() ? dirtyLeafNodes : dirtyInternalNodes).put(nodeid, node);
		(node.isLeaf() ? cacheLeafNodes : cacheInternalNodes).remove(nodeid);
		if (enableIOStats) getIOStat(nodeid).incCacheWrite();
		if (enableDirtyCheck) dirtyCheck.set(index);
	}

	/**
	 * Write all dirty nodes
	 */
	public synchronized void sync() {
		if (!validState) throw new InvalidStateException();
		try {
			privateSync(true);
		}
		finally {
			releaseNodes();
		}
	}

	/**
	 * set callback called when buffers where synched to disk
	 * @param callback
	 */
	public void setCallback(final CallbackSync callback) {
		storage.setCallback(callback);
	}

	/**
	 * Write all dirty nodes
	 */
	@SuppressWarnings("unchecked")
	private void privateSync(final boolean syncInternal) {
		final long ts = System.currentTimeMillis();
		Node<K, V>[] dirtyBlocks;
		//
		// Write Leaf Nodes
		dirtyBlocks = dirtyLeafNodes.getValues();
		Arrays.sort(dirtyBlocks, dirtyComparatorByID);
		for (Node<K, V> node : dirtyBlocks) {
			if (node == null) break;
			//if (DEBUG) System.out.println("node.id=" + node.id);
			dirtyLeafNodes.remove(node.id);
			putNodeToStore(node);
			if (!node.isDeleted()) { 
				cacheLeafNodes.put(node.id, node);
			}
		}
		if (!dirtyLeafNodes.isEmpty()) dirtyLeafNodes.clear(false); // Clear without shrink
		// Write Internal Nodes
		if (syncInternal) {
			dirtyBlocks = dirtyInternalNodes.getValues();
			Arrays.sort(dirtyBlocks, dirtyComparatorByID);
			for (Node<K, V> node : dirtyBlocks) {
				if (node == null) break;
				//if (DEBUG) System.out.println("node.id=" + node.id);
				dirtyInternalNodes.remove(node.id);
				putNodeToStore(node);
				if (!node.isDeleted()) { 
					cacheInternalNodes.put(node.id, node);
				}
			}
			if (!dirtyInternalNodes.isEmpty()) dirtyInternalNodes.clear(false); // Clear without shrink
		}
		//
		writeMetaData(false);
		storage.sync();
		redoQueue.clear();
		redoStore.clear();
		if (DEBUG) {
			StringBuilder sb = new StringBuilder();
			sb
			.append(this.getClass().getName()).append("::sync()")
			.append(" elements=").append(elements)
			.append(" Int=").append(maxInternalNodes)
			.append(" Leaf=").append(maxLeafNodes)
			.append(" dirty{")
			.append(" Int=").append(dirtyInternalNodes.size())
			.append(" Leaf=").append(dirtyLeafNodes.size())
			.append(" }")
			.append(" cache{")
			.append(" Int=").append(cacheInternalNodes.size())
			.append(" Leaf=").append(cacheLeafNodes.size())
			.append(" }")
			.append(" storage{")
			.append(" total=").append(storage.sizeInBlocks())
			.append(" free=").append(freeBlocks.cardinality())
			.append(" }")
			.append(" time=").append(System.currentTimeMillis() - ts);
			System.out.println(sb.toString());
		}
		//clearWriteCaches();
	}

	/**
	 * Clear write caches without sync dirty nodes
	 */
	private final void clearWriteCaches() {
		// Clear without shrink
		dirtyInternalNodes.clear(false);
		dirtyLeafNodes.clear(false);
	}

	// ===================================== POOL BUFFER

	/**
	 * Use DirectByteBuffer/true or HeapByteBuffer/false ?
	 */
	private final boolean isDirect = true; // use DirectByteBuffer o HeapByteBuffer

	/**
	 * Pool of ByteBuffers
	 */
	private final BufferStacker bufstack;

	// ===================================== STATS

	/**
	 * I/O Stats of nodes  
	 */
	private final IntHashMap<IOStat> iostats = new IntHashMap<IOStat>(256, IOStat.class);

	/**
	 * Max allocated Internal nodes
	 */
	private int maxInternalNodes = 0;

	/**
	 * Max allocated Leaf Nodes
	 */
	private int maxLeafNodes = 0;

	/**
	 * Return or Create if not exist an IOStat object for a nodeid
	 * @param nodeid
	 * @param isLeaf
	 * @return IOStat object
	 */
	private IOStat getIOStat(final int nodeid) {
		IOStat io = iostats.get(nodeid);
		if (io == null) {
			io = new IOStat(nodeid);
			iostats.put(nodeid, io);
		}
		return io;
	}

	/**
	 * Dump IOStats of tree to a file
	 * @param file
	 * @throws FileNotFoundException
	 */
	public void dumpStats(final String file) throws FileNotFoundException {
		PrintStream out = null;
		try {
			out = new PrintStream(new FileOutputStream(file));
			dumpStats(out);
		}
		finally {
			try { out.close(); } catch(Exception ign) {}
		}
	}

	/**
	 * Dump IOStats of tree to PrintStream (System.out?)
	 * @param out PrintStream
	 */
	@SuppressWarnings("unused")
	public synchronized void dumpStats(final PrintStream out) {
		if (!validState) throw new InvalidStateException();

		out.println("=== Stats ===");
		out.println("maxAllocatedInternalNodes=" + maxInternalNodes);
		out.println("maxAllocatedLeafNodes=" + maxLeafNodes);
		out.println("readCacheSizeInternalNodes=" + readCacheInternal);
		out.println("readCacheSizeLeafNodes=" + readCacheLeaf);
		out.println("leafNodeSize=" + leafNodeFactory.getStructMaxSize());
		out.println("internalNodeSize=" + internalNodeFactory.getStructMaxSize());
		out.println("blockSize=" + blockSize);
		out.println("currentCacheSize=" + (maxCacheSizeInBytes / 1024 / 1024) + "MB");
		out.println("minRecomendedCacheSize=" + ((blockSize * (maxInternalNodes + maxLeafNodes)) / 1024 / 1024) + "MB");

		if (!enableIOStats) {
			out.println("=== IOStats ===");
			out.println("not enabled");
			return;
		}
		//
		final IOStat[] ios = iostats.getValues();
		final Comparator<IOStat> ioComparator = new Comparator<IOStat>() {
			@Override
			public int compare(final IOStat o1, final IOStat o2) {
				if (o1 == null) {
					if (o2 == null) return 0; // o1 == null & o2 == null
					return 1; // o1 == null & o2 != null
				}
				if (o2 == null) return -1; // o1 != null & o2 == null
				final long thisVal = ((o1.id < 0 ? 0 : 1)<<63) + (o1.physRead<<16) + o1.physWrite; // o1.id;
				final long anotherVal = ((o2.id < 0 ? 0 : 1)<<63) + (o2.physRead<<16) + o2.physWrite; // o2.id;
				return ((thisVal<anotherVal) ? -1 : ((thisVal==anotherVal) ? 0 : 1));
			}
		};
		Arrays.sort(ios, ioComparator);
		for (final IOStat io : ios) {
			if (io == null) break;
			out.println(io.toString());
		}
	}

	/**
	 * Class to hold I/O stats of nodes (physical read/write), (cache read/write) 
	 */
	private static class IOStat {
		public final int id;
		public int physRead = 0;
		public int physWrite = 0;
		public int cacheRead = 0;
		public int cacheWrite = 0;
		//
		public IOStat(final int nodeid) {
			this.id = nodeid;
		}
		public IOStat incPhysRead() {
			physRead++;
			return this;
		}
		public IOStat incPhysWrite() {
			physWrite++;
			return this;
		}
		public IOStat incCacheRead() {
			cacheRead++;
			return this;
		}
		public IOStat incCacheWrite() {
			cacheWrite++;
			return this;
		}
		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb
			.append(Node.isLeaf(id) ? "L" : "I")
			.append(id < 0 ? -id : id)
			.append(" pr=").append(physRead)
			.append(" pw=").append(physWrite)
			.append(" cr=").append(cacheRead)
			.append(" cw=").append(cacheWrite);
			return sb.toString();
		}
	}

}
