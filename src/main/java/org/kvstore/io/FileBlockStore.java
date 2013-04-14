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
package org.kvstore.io;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.ref.Reference;
import java.lang.ref.SoftReference;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.log4j.Logger;
import org.kvstore.pool.BufferStacker;
import org.kvstore.structures.hash.IntHashMap;
import org.kvstore.utils.Check64bitsJVM;

/**
 * File based Storage of fixed size blocks 
 * This class is NOT Thread-Safe
 *
 * @author Guillermo Grandes / guillermo.grandes[at]gmail.com
 */
public class FileBlockStore {
	private static final Logger log = Logger.getLogger(FileBlockStore.class);
	/**
	 * Size of block
	 */
	public final int blockSize;
	/**
	 * File associated to this store
	 */
	private File file = null;
	/**
	 * RamdomAccessFile for this store
	 */
	private RandomAccessFile raf = null;
	/**
	 * FileChannel for this store
	 */
	private FileChannel fileChannel = null;
	/**
	 * Support for mmap
	 */
	private boolean useMmap = false;
	/**
	 * ByteBuffer pool
	 */
	private final BufferStacker bufstack;
	/**
	 * In Valid State?
	 */
	private boolean validState = false;
	/**
	 * Callback called when flush buffers to disk
	 */
	private CallbackSync callback = null;

	/**
	 * Instantiate FileBlockStore
	 * @param file name of file to open
	 * @param blockSize size of block
	 * @param isDirect use DirectByteBuffer or HeapByteBuffer?
	 */
	public FileBlockStore(final String file, final int blockSize, final boolean isDirect) {
		this(new File(file), blockSize, isDirect);
	}

	/**
	 * Instantiate FileBlockStore
	 * @param file file to open
	 * @param blockSize size of block
	 * @param isDirect use DirectByteBuffer or HeapByteBuffer?
	 */
	public FileBlockStore(final File file, final int blockSize, final boolean isDirect) {
		this.file = file;
		this.blockSize = blockSize;
		this.bufstack = BufferStacker.getInstance(blockSize, isDirect);
	}

	// ========= Open / Close =========

	/**
	 * Open file
	 * @return true if valid state
	 */
	public boolean open() {
		if (isOpen()) {
			close();
		}
		if (log.isDebugEnabled())
			log.debug("open("+file+")");
		try {
			raf = new RandomAccessFile(file, "rw");
			fileChannel = raf.getChannel();
		}
		catch(Exception e) {
			log.error("Exception in open()", e);
			try { fileChannel.close(); } catch(Exception ign) {}
			try { raf.close(); } catch(Exception ign) {}
			raf = null;
			fileChannel = null;
		}
		validState = isOpen();
		return validState;
	}

	/**
	 * Close file
	 */
	public void close() {
		mmaps.clear(false);
		try { fileChannel.close(); } catch(Exception ign) {}
		try { raf.close(); } catch(Exception ign) {}
		fileChannel = null;
		raf = null;
		validState = false;
	}

	// ========= Info =========

	/**
	 * @return size of block
	 */
	public int getBlockSize() {
		return blockSize;
	}

	/**
	 * @return true if file is open
	 */
	public boolean isOpen() {
		try { 
			if (fileChannel != null) return fileChannel.isOpen();
		} catch(Exception ign) {}
		return false;
	}

	/**
	 * @return size of file in blocks
	 * @see #getBlockSize()
	 */
	public int sizeInBlocks() {
		try {
			final long len = file.length();
			final long num_blocks = ((len / blockSize) + (((len % blockSize) == 0) ? 0 : 1));
			if (log.isDebugEnabled())
				log.debug("size()=" + num_blocks);
			return (int) num_blocks;
		}
		catch(Exception e) {
			log.error("Exception in sizeInBlocks()", e);
		}
		return -1;
	}

	// ========= Destroy =========

	/**
	 * Truncate file
	 */
	public void clear() {
		if (!validState) throw new InvalidStateException();
		try {
			fileChannel.position(0).truncate(0);
			sync();
		}
		catch(Exception e) {
			log.error("Exception in clear()", e);
		}
	}

	/**
	 * Delete file
	 */
	public void delete() {
		close();
		try { file.delete(); } catch(Exception ign) {}
	}

	// ========= Operations =========

	/**
	 * set callback called when buffers where synched to disk
	 * @param callback
	 */
	public void setCallback(final CallbackSync callback) {
		this.callback = callback;
	}

	/**
	 * Read block from file
	 * @param index of block
	 * @return ByteBuffer from pool with data
	 */
	public ByteBuffer get(final int index) {
		if (!validState) throw new InvalidStateException();
		if (log.isDebugEnabled())
			log.debug("get("+index+")");
		try {
			//final ByteBuffer buf = ByteBuffer.allocate(ELEMENT_SIZE);
			final ByteBuffer buf = bufstack.pop();
			if (useMmap) {
				final MappedByteBuffer mbb = getMmapForIndex(index);
				if (mbb != null) {
					buf.put(mbb);
					return buf;
				}
				// Fallback to RAF
			}
			fileChannel.position(index * blockSize).read(buf);
			buf.rewind();
			return buf;
		}
		catch(Exception e) {
			log.error("Exception in get("+index+")", e);
		}
		return null;
	}

	/**
	 * Write from buf to file
	 * @param index of block
	 * @param buf ByteBuffer to write (this will be send to pool after write)
	 * @return true if write is OK
	 */
	public boolean set(final int index, final ByteBuffer buf) {
		if (!validState) throw new InvalidStateException();
		if (log.isDebugEnabled())
			log.debug("set("+index+","+buf+")");
		try {
			if (buf.limit() > blockSize) {
				log.error("ERROR: buffer.capacity="+buf.limit()+" > blocksize=" + blockSize);
			}
			if (useMmap) {
				final MappedByteBuffer mbb = getMmapForIndex(index);
				if (mbb != null) {
					mbb.put(buf);
					return true;
				}
				// Fallback to RAF
			}
			fileChannel.position(index * blockSize).write(buf);
			bufstack.push(buf);
			return true;
		}
		catch(Exception e) {
			log.error("Exception in set("+index+")", e);
		}
		return false;
	}

	/**
	 * Forces any updates to this file to be written to the storage device that contains it. 
	 */
	public void sync() {
		if (!validState) throw new InvalidStateException();
		if (useMmap) {
			syncAllMmaps();
		}
		if (fileChannel != null) {
			try { fileChannel.force(false); } catch(Exception ign) {}
		}
		if (callback != null)
			callback.synched();
	}

	public static interface CallbackSync {
		public void synched();
	}

	// ========= Mmap ===============

	private static final boolean useSegments = true;
	private static final int segmentSize = (32 * 4096); // N_PAGES * PAGE=4KB // 128KB
	@SuppressWarnings("rawtypes")
	private final IntHashMap<BufferReference> mmaps = new IntHashMap<BufferReference>(128, BufferReference.class);
	/**
	 * Comparator for write by Idx
	 */
	private Comparator<BufferReference<MappedByteBuffer>> comparatorByIdx = new Comparator<BufferReference<MappedByteBuffer>>() {
		@Override
		public int compare(final BufferReference<MappedByteBuffer> o1, final BufferReference<MappedByteBuffer> o2) {
			if (o1 == null) {
				if (o2 == null) return 0; // o1 == null & o2 == null
				return 1; // o1 == null & o2 != null
			}
			if (o2 == null) return -1; // o1 != null & o2 == null
			final int thisVal = (o1.idx < 0 ? -o1.idx : o1.idx);
			final int anotherVal = (o2.idx < 0 ? -o2.idx : o2.idx);
			return ((thisVal<anotherVal) ? -1 : ((thisVal==anotherVal) ? 0 : 1));
		}
	};

	/**
	 * Is enabled mmap for this store?
	 * @return true/false
	 */
	public boolean useMmap() {
		return useMmap;
	}
	/**
	 * Enable mmap of files (default is not enabled), call before use {@link #open()}
	 * <p/>
	 * Recommended use of: {@link #enableMmapIfSupported()}
	 * <p/>
	 * <b>NOTE:</b> 32bit JVM can only address 2GB of memory, enable mmap can throw <b>java.lang.OutOfMemoryError: Map failed</b> exceptions
	 */
	public void enableMmap() {
		if (validState) throw new InvalidStateException();
		useMmap = true;
	}
	/**
	 * Enable mmap of files (default is not enabled) if JVM is 64bits, call before use {@link #open()}
	 */
	public void enableMmapIfSupported() {
		if (validState) throw new InvalidStateException();
		useMmap = Check64bitsJVM.JVMis64bits();
	}

	private final int addressIndexToSegment(final int index) {
		return (int)(((long)index * blockSize) / segmentSize);
	}
	private final int addressIndexToSegmentOffset(final int index) {
		return (index % (segmentSize / blockSize));
	}

	public final MappedByteBuffer getMmapForIndex(final int index) {
		final int mapIdx = (useSegments ? addressIndexToSegment(index) : index);
		final int mapSize = (useSegments ? segmentSize : blockSize);
		try {
			@SuppressWarnings("unchecked")
			final Reference<MappedByteBuffer> bref = mmaps.get(mapIdx);
			MappedByteBuffer mbb = null;
			if (bref != null) {
				mbb = bref.get();
			}
			if (mbb == null) { // Create mmap
				final long mapOffset = ((long)mapIdx * mapSize);
				mbb = fileChannel.map(FileChannel.MapMode.READ_WRITE, mapOffset, mapSize);
				//mbb.load();
				mmaps.put(mapIdx, new BufferReference<MappedByteBuffer>(mapIdx, mbb));
				//log.info("Mapped index=" + index + " to offset=" + mapOffset + " size=" + mapSize);
			} else {
				mbb.clear();
			}
			if (useSegments) { // slice segment
				final int sliceBegin = (addressIndexToSegmentOffset(index) * blockSize);
				final int sliceEnd = (sliceBegin + blockSize);
				//log.info("Mapping segment=" + mapIdx + " index=" + index + " sliceBegin=" + sliceBegin + " sliceEnd=" + sliceEnd);
				mbb.limit(sliceEnd);
				mbb.position(sliceBegin);
				mbb = (MappedByteBuffer) mbb.slice();
			}
			return mbb;
		} catch (IOException e) {
			log.error("IOException in getMmapForIndex("+index+")", e);
		}
		return null;
	}

	private void syncAllMmaps() {
		@SuppressWarnings("unchecked")
		final BufferReference<MappedByteBuffer>[] maps = mmaps.getValues();
		Arrays.sort(maps, comparatorByIdx);
		int synched = 0;
		for (final Reference<MappedByteBuffer> ref : maps) {
			if (ref == null) break;
			final MappedByteBuffer mbb = ref.get();
			if (mbb != null) {
				try { mbb.force(); } catch(Exception ign) {}
				synched++;
			}
		}
		System.out.println("syncAllMmaps() synched=" + synched);
	}

	static class BufferReference<T extends MappedByteBuffer> extends SoftReference<T> {
		final int idx;
		public BufferReference(final int idx, final T referent) {
			super(referent);
			this.idx = idx;
		}
	}

	// ========= Exceptions =========

	/**
	 * Exception throwed when store is in invalid state (closed) 
	 */
	public static class InvalidStateException extends RuntimeException {
		private static final long serialVersionUID = 42L;
	}

	// ========= END =========
}
