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
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import org.kvstore.pool.BufferStacker;

/**
 * File based Storage of fixed size blocks 
 * This class is NOT Thread-Safe
 *
 * @author Guillermo Grandes / guillermo.grandes[at]gmail.com
 */
public class FileBlockStore {
	private static final boolean DEBUG = false;
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
	 * Support for mmap (not fully implemented)
	 */
	private MappedByteBuffer mbb = null;
	/**
	 * ByteBuffer pool
	 */
	private final BufferStacker bufstack;
	/**
	 * In Valid State?
	 */
	private boolean validState = false;

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
		if (DEBUG) System.out.println("open("+file+")");
		try {
			raf = new RandomAccessFile(file, "rw");
			fileChannel = raf.getChannel();
		}
		catch(Exception e) {
			e.printStackTrace();
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
		try { fileChannel.close(); } catch(Exception ign) {}
		try { raf.close(); } catch(Exception ign) {}
		mbb = null;
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
			if (DEBUG) System.out.println("size()=" + num_blocks);
			return (int) num_blocks;
		}
		catch(Exception e) {
			e.printStackTrace();
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
			fileChannel.position(0);
			fileChannel.truncate(0);
			sync();
		}
		catch(Exception e) {
			e.printStackTrace();
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
	 * Read block from file
	 * @param index of block
	 * @return ByteBuffer from pool with data
	 */
	public ByteBuffer get(final int index) {
		if (!validState) throw new InvalidStateException();
		if (DEBUG) System.out.println("get("+index+")");
		try {
			//final ByteBuffer buf = ByteBuffer.allocate(ELEMENT_SIZE);
			final ByteBuffer buf = bufstack.pop();
			if (mbb != null) {
				mbb.limit((index + 1) * blockSize);
				mbb.position(index * blockSize);
				buf.put(mbb);
			} else {
				fileChannel.position(index * blockSize);
				fileChannel.read(buf);
			}
			buf.rewind();
			return buf;
		}
		catch(Exception e) {
			e.printStackTrace();
			System.out.println("index=" + index);
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
		if (DEBUG) System.out.println("set("+index+","+buf+")");
		try {
			if (buf.limit() > blockSize) {
				System.err.println("ERROR: buffer.capacity="+buf.limit()+" > blocksize=" + blockSize);
			}
			if (mbb != null) {
				mbb.limit((index + 1) * blockSize);
				mbb.position(index * blockSize);
				mbb.put(buf);
			} else {
				fileChannel.position(index * blockSize);
				fileChannel.write(buf);
			}
			bufstack.push(buf);
			return true;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		return false;
	}

	/**
	 * Forces any updates to this file to be written to the storage device that contains it. 
	 */
	public void sync() {
		if (!validState) throw new InvalidStateException();
		if (mbb != null) {
			try { mbb.force(); } catch(Exception ign) {}
		}
		if (fileChannel != null) {
			try { fileChannel.force(false); } catch(Exception ign) {}
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
