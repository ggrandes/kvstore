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
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * File for store one Long value with history for crash recovery
 * This class is Thread-Safe
 *
 * @author Guillermo Grandes / guillermo.grandes[at]gmail.com
 */
public class FileLongStore {
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
	private FileChannel fc = null;
	/**
	 * ByteBuffer (internal used)
	 */
	private final ByteBuffer buf = ByteBuffer.allocate(8);
	/**
	 * In Valid State?
	 */
	private boolean validState = false;
	/**
	 * Current Long
	 */
	private long value = 0;

	/**
	 * Instantiate FileLongPointerStore
	 * @param file name of file to open
	 */
	public FileLongStore(final String file) {
		this(new File(file));
	}

	/**
	 * Instantiate FileLongPointerStore
	 * @param file file to open
	 */
	public FileLongStore(final File file) {
		this.file = file;
	}

	// ========= Open / Close =========

	/**
	 * Open file
	 * @return true if valid state
	 */
	public synchronized boolean open() {
		if (isOpen()) {
			close();
		}
		try {
			raf = new RandomAccessFile(file, "rw");
			fc = raf.getChannel();
		}
		catch(Exception e) {
			e.printStackTrace();
			try { close(); } catch(Exception ign) {}
		}
		validState = isOpen();
		return validState;
	}

	/**
	 * Close file
	 */
	public synchronized void close() {
		if (validState) sync();
		try { fc.close(); } catch(Exception ign) {}
		try { raf.close(); } catch(Exception ign) {}
		raf = null;
		fc = null;
		//
		validState = false;
	}

	// ========= Info =========

	/**
	 * @return true if file is open
	 */
	public synchronized boolean isOpen() {
		try { 
			if (fc != null) {
				return fc.isOpen();
			}
		} catch(Exception ign) {}
		return false;
	}

	/**
	 * Read value from file
	 * @throws IOException 
	 */
	public synchronized boolean canRead() throws IOException {
		if (!validState) throw new InvalidStateException();
		final long offset = ((fc.size() & ~7) -8);
		return (offset >= 0);
	}

	/**
	 * @return size of file in bytes
	 * @see #getBlockSize()
	 */
	public synchronized long size() {
		try {
			return file.length();
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
	public synchronized void clear() {
		if (!validState) throw new InvalidStateException();
		try {
			buf.clear();
			fc.position(0).truncate(0).force(true);
			close();
			open();
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Delete file
	 */
	public synchronized void delete() {
		buf.clear();
		close();
		try { file.delete(); } catch(Exception ign) {}
	}

	// ========= Operations =========

	/**
	 * Set value
	 */
	public synchronized void set(final long value) {
		this.value = value;
	}
	/**
	 * Get value
	 */
	public synchronized long get() {
		return value;
	}

	/**
	 * Read value from file
	 * @throws IOException 
	 */
	public synchronized void read() throws IOException {
		if (!validState) throw new InvalidStateException();
		final long offset = ((fc.size() & ~7) -8);
		if (offset < 0)
			throw new IOException("Empty file");
		buf.clear();
		int readed = fc.position(offset).read(buf);
		if (readed < 8) { // long 8 bytes
			throw new IOException("cant read long from file");
		}
		buf.flip();
		value = buf.getLong();
	}

	/**
	 * Write value to file
	 * @throws IOException 
	 */
	public void write() throws IOException {
		write(false);
	}

	/**
	 * Write value to file
	 * @param forceSync if true data must be synced to disk
	 * @throws IOException 
	 */
	public synchronized void write(final boolean forceSync) throws IOException {
		if (!validState) throw new InvalidStateException();
		buf.clear();
		buf.putLong(value);
		buf.flip();
		fc.position(fc.size()).write(buf); // go end and write
		if (forceSync)
			fc.force(false);
	}

	/**
	 * Write value to file and reduce size to minimal
	 * @throws IOException 
	 */
	public synchronized void pack() throws IOException {
		if (!validState) throw new InvalidStateException();
		buf.clear();
		buf.putLong(value);
		buf.flip();
		fc.position(0).write(buf); // go begin and write
		fc.truncate(8).force(true);
	}

	/**
	 * Forces any updates to this file to be written to the storage device that contains it.
	 * @return false if exception occur 
	 */
	public synchronized boolean sync() {
		if (!validState) throw new InvalidStateException();
		try { 
			fc.force(false);
			return true;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		return false;
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
