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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.apache.log4j.Logger;

/**
 * File based Stream Storage
 * This class is Thread-Safe
 *
 * @author Guillermo Grandes / guillermo.grandes[at]gmail.com
 */
public final class FileStreamStore {
	private static final Logger log = Logger.getLogger(FileStreamStore.class);
	private final static short MAGIC = 0x754C;
	private final static byte MAGIC_PADDING = 0x42;
	private final static byte MAGIC_FOOT = 0x24;
	private static final int HEADER_LEN = 6;
	private static final int FOOTER_LEN = 1;

	/**
	 * File associated to this store
	 */
	private File file = null;
	/**
	 * Size/Power-of-2 for size of buffers/align
	 * ^9=512 ^12=4096 ^16=65536
	 */
	private final int bits;

	/**
	 * RamdomAccessFile for Input this store
	 */
	private RandomAccessFile rafInput = null;
	/**
	 * FileChannel for Input this store
	 */
	private FileChannel fcInput = null;
	/**
	 * ByteBuffer for Input (internal used)
	 */
	private final ByteBuffer bufInput;

	/**
	 * FileOutputStream for Output this store
	 */
	private FileOutputStream osOutput = null;
	/**
	 * FileChannel for Output this store
	 */
	private FileChannel fcOutput = null;
	/**
	 * Current output offset for blocks (commited to disk)
	 */
	private long offsetOutputCommited = 0;
	/**
	 * Current output offset for blocks (uncommited to disk) 
	 */
	private long offsetOutputUncommited = 0;
	/**
	 * ByteBuffer for Output (internal used)
	 */
	private final ByteBuffer bufOutput;
	/**
	 * In Valid State?
	 */
	private boolean validState = false;
	/**
	 * flush buffer on every write?
	 */
	private boolean flushOnWrite = false;
	/**
	 * sync to disk on flushbuffer?
	 */
	private boolean syncOnFlush = true;
	/**
	 * align data to buffer boundary?
	 */
	private boolean alignBlocks = true;
	/**
	 * Callback called when flush buffers to disk
	 */
	private CallbackSync callback = null;

	/**
	 * Instantiate FileStreamStore
	 * @param file name of file to open
	 * @param size for buffer to reduce context switching (minimal is 512bytes, recommended 64KBytes)
	 */
	public FileStreamStore(final String file, final int bufferSize) {
		this(new File(file), bufferSize);
	}

	/**
	 * Instantiate FileStreamStore
	 * @param file file to open
	 * @param size for buffer to reduce context switching (minimal is 512bytes) 
	 */
	public FileStreamStore(final File file, final int bufferSize) {
		this.file = file;
		this.bits = ((int)Math.ceil(Math.log(Math.max(bufferSize, 512))/Math.log(2))); // round to power of 2
		this.bufInput = ByteBuffer.allocate(512); // default HDD sector size
		this.bufOutput = ByteBuffer.allocate(1 << bits);
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
		if (log.isDebugEnabled())
			log.debug("open("+file+")");
		try {
			osOutput = new FileOutputStream(file, true);
			fcOutput = osOutput.getChannel();
			rafInput = new RandomAccessFile(file, "r");
			fcInput = rafInput.getChannel();
			offsetOutputUncommited = offsetOutputCommited = fcOutput.size();
		}
		catch(Exception e) {
			log.error("Exception in open()", e);
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
		try { fcInput.close(); } catch(Exception ign) {}
		try { rafInput.close(); } catch(Exception ign) {}
		try { osOutput.close(); } catch(Exception ign) {}
		try { fcOutput.close(); } catch(Exception ign) {}
		rafInput = null;
		fcInput = null;
		osOutput = null;
		fcOutput = null;
		//
		validState = false;
	}

	// ========= Info =========

	/**
	 * @return true if file is open
	 */
	public synchronized boolean isOpen() {
		try { 
			if ((fcInput != null) && (fcOutput != null)) {
				return (fcInput.isOpen() && fcOutput.isOpen());
			}
		} catch(Exception ign) {}
		return false;
	}

	/**
	 * @return size of file in bytes
	 * @see #getBlockSize()
	 */
	public synchronized long size() {
		try {
			return (file.length() + bufOutput.position());
		}
		catch(Exception e) {
			log.error("Exception in size()", e);
		}
		return -1;
	}

	/**
	 * check if empty
	 * @return true if empty
	 */
	public synchronized boolean isEmpty() {
		if (!validState) throw new InvalidStateException();
		return (size() == 0);
	}

	/**
	 * Read end of valid and check last magic footer
	 * @return true if valid
	 */
	public synchronized boolean isValid() {
		if (!validState) throw new InvalidStateException();
		final long size = size();
		if (size == 0) return true;
		try {
			final long offset = (size - FOOTER_LEN);
			if (offset < 0) return false;
			if (offset >= offsetOutputCommited) {
				if (bufOutput.position() > 0) {
					log.warn("WARN: autoflush forced");
					flushBuffer();
				}
			}
			bufInput.clear();
			bufInput.limit(FOOTER_LEN);
			final int readed = fcInput.position(offset).read(bufInput);
			if (readed < FOOTER_LEN) {
				return false;
			}
			bufInput.flip();
			final int footer = bufInput.get(); 	// Footer (byte)
			if (footer != MAGIC_FOOT) {
				log.error("MAGIC FOOT fake=" + Integer.toHexString(footer) + " expected=" + Integer.toHexString(MAGIC_FOOT));
				return false;
			}
			return true;
		}
		catch(Exception e) {
			log.error("Exception in isValid()", e);
		}
		return false;
	}

	// ========= Destroy =========

	/**
	 * Truncate file
	 */
	public synchronized void clear() {
		if (!validState) throw new InvalidStateException();
		try {
			bufOutput.clear();
			fcOutput.position(0).truncate(0).force(true);
			offsetOutputUncommited = offsetOutputCommited = fcOutput.position();
			if (callback != null)
				callback.synched(offsetOutputCommited);
			close();
			open();
		}
		catch(Exception e) {
			log.error("Exception in clear()", e);
		}
	}

	/**
	 * Delete file
	 */
	public synchronized void delete() {
		bufOutput.clear();
		close();
		try { file.delete(); } catch(Exception ign) {}
	}

	// ========= Operations =========

	/**
	 * set flush buffer on write to true/false, default false
	 * @param syncOnFlush
	 */
	public synchronized void setFlushOnWrite(final boolean flushOnWrite) {
		this.flushOnWrite = flushOnWrite;
	}
	/**
	 * set sync to disk flush buffer to true/false, default true
	 * @param syncOnFlush
	 */
	public synchronized void setSyncOnFlush(final boolean syncOnFlush) {
		this.syncOnFlush = syncOnFlush;
	}
	/**
	 * set align blocks to buffer boundary to true/false, default true
	 * @param alignBlocks
	 */
	public synchronized void setAlignBlocks(final boolean alignBlocks) {
		this.alignBlocks = alignBlocks;
	}
	/**
	 * set callback called when buffers where synched to disk
	 * @param callback
	 */
	public synchronized void setCallback(final CallbackSync callback) {
		this.callback = callback;
	}

	/**
	 * Read desired block of datalen from end of file
	 * @param datalen expected
	 * @param ByteBuffer
	 * @return new offset (offset+headerlen+datalen+footer)
	 */
	public synchronized long readFromEnd(final long datalen, final ByteBuffer buf) {
		if (!validState) throw new InvalidStateException();
		final long size = size();
		final long offset = (size - HEADER_LEN - datalen - FOOTER_LEN);
		return read(offset, buf);
	}

	/**
	 * Read block from file
	 * @param offset of block
	 * @param ByteBuffer
	 * @return new offset (offset+headerlen+datalen+footer)
	 */
	public synchronized long read(long offset, final ByteBuffer buf) {
		if (!validState) throw new InvalidStateException();
		try {
			int readed;
			while (true) {
				if (offset >= offsetOutputCommited) {
					if (bufOutput.position() > 0) {
						log.warn("WARN: autoflush forced");
						flushBuffer();
					}
				}
				bufInput.clear();
				readed = fcInput.position(offset).read(bufInput); // Read 1 sector
				if (readed < HEADER_LEN) { // short+int (6 bytes)
					return -1;
				}
				bufInput.flip();
				final int magicB1 = (bufInput.get() & 0xFF); 	// Header - Magic (short, 2 bytes, msb-first)
				final int magicB2 = (bufInput.get() & 0xFF); 	// Header - Magic (short, 2 bytes, lsb-last)
				if (alignBlocks && (magicB1 == MAGIC_PADDING)) {
					final int diffOffset = nextBlockBoundary(offset);
					if (diffOffset > 0) {
						//						log.info("WARN: skipping " + diffOffset + "bytes to next block-boundary");
						offset += diffOffset;
						continue;
					}
				}
				final int magic = ((magicB1 << 8) | magicB2);
				if (magic != MAGIC) {
					log.error("MAGIC HEADER fake=" + Integer.toHexString(magic) + " expected=" + Integer.toHexString(MAGIC));
					return -1;
				}
				break;
			}
			//
			final int datalen = bufInput.getInt(); 	// Header - Data Size (int, 4 bytes)
			final int footer = bufInput.get(datalen+HEADER_LEN); 	// Footer (byte)
			if (footer != MAGIC_FOOT) {
				log.error("MAGIC FOOT fake=" + Integer.toHexString(footer) + " expected=" + Integer.toHexString(MAGIC_FOOT));
				return -1;
			}
			bufInput.limit(Math.min(readed, datalen+HEADER_LEN));
			buf.put(bufInput);
			if (datalen > (readed-HEADER_LEN)) {
				buf.limit(datalen);
				readed = fcInput.read(buf);
			}
			buf.flip();
			return (offset+HEADER_LEN+datalen+FOOTER_LEN);
		}
		catch(Exception e) {
			log.error("Exception in read("+offset+")", e);
		}
		return -1;
	}

	/**
	 * Write from buf to file
	 * 
	 * @param offset of block
	 * @param buf ByteBuffer to write
	 * @return long offset where buffer begin was write or -1 if error
	 */
	public synchronized long write(final ByteBuffer buf) {
		if (!validState) throw new InvalidStateException();
		final int packet_size = (HEADER_LEN + buf.limit() + FOOTER_LEN); // short + int + data + byte
		final boolean useDirectIO = (packet_size > (1<<bits));
		try {
			if (useDirectIO) {
				log.warn("WARN: usingDirectIO packet size is greater ("+packet_size+") than file buffer (" + bufOutput.capacity() + ")");
			}
			// Align output
			if (alignBlocks && !useDirectIO) {
				final int diffOffset = nextBlockBoundary(offsetOutputUncommited);
				if (packet_size > diffOffset) {
					//log.warn("WARN: aligning offset=" + offsetOutputUncommited + " to=" + (offsetOutputUncommited+diffOffset) + " needed=" + packet_size + " allowed=" + diffOffset);
					alignBuffer(diffOffset);
					offsetOutputUncommited += diffOffset;
				}
			}
			// Remember current offset
			final long offset = offsetOutputUncommited;
			// Write pending buffered data to disk
			if (bufOutput.remaining() < packet_size) {
				flushBuffer();
			}
			// Write new data to buffer
			bufOutput.put((byte)((MAGIC>>8) & 0xFF)); 	// Header - Magic (short, 2 bytes, msb-first)
			bufOutput.put((byte)(MAGIC & 0xFF)); 		// Header - Magic (short, 2 bytes, lsb-last)
			bufOutput.putInt(buf.limit()); 				// Header - Data Size (int, 4 bytes)
			if (useDirectIO) {
				bufOutput.flip();
				fcOutput.write(new ByteBuffer[] { bufOutput, buf, ByteBuffer.wrap(new byte[] { MAGIC_FOOT }) }); // Write Header + Data + Footer
				bufOutput.clear();
				offsetOutputUncommited = offsetOutputCommited = fcOutput.position();
				if (syncOnFlush) {
					fcOutput.force(false);
					if (callback != null)
						callback.synched(offsetOutputCommited);
				}
			}
			else {
				bufOutput.put(buf); // Data Body
				bufOutput.put(MAGIC_FOOT); // Footer
				// Increment offset of buffered data (header + user-data)
				offsetOutputUncommited += packet_size;
				if (flushOnWrite)
					flushBuffer();
			}
			//
			return offset;
		}
		catch(Exception e) {
			log.error("Exception in write()", e);
		}
		return -1L;
	}

	/**
	 * How much bytes left to next block boundary
	 * @param offset
	 * @return bytes left
	 */
	private final int nextBlockBoundary(final long offset) {
		return (int)((((offset >> bits) + 1) << bits) - offset);
	}

	/**
	 * Pad output buffer with NULL to complete alignment
	 * @param diff bytes
	 * @throws IOException
	 */
	private final void alignBuffer(final int diff) throws IOException {
		if (bufOutput.remaining() < diff) {
			flushBuffer();
		}
		bufOutput.put(MAGIC_PADDING); // Magic for Padding
		int i = 1;
		for (; i+8 <= diff; i+=8) {
			bufOutput.putLong(0L);
		}
		for (; i+4 <= diff; i+=4) {
			bufOutput.putInt(0);
		}
		switch(diff-i) {
		case 3: 
			bufOutput.put((byte)0);
		case 2: 
			bufOutput.putShort((short)0);
			break;
		case 1: 
			bufOutput.put((byte)0);
		}
	}

	/**
	 * Flush buffer to file
	 * @return false if exception occur 
	 */
	public synchronized boolean flush() {
		if (!validState) throw new InvalidStateException();
		try { 
			flushBuffer();
			return true;
		}
		catch(Exception e) {
			log.error("Exception in flush()", e);
		}
		return false;
	}

	/**
	 * Write uncommited data to disk
	 * @throws IOException
	 */
	private final void flushBuffer() throws IOException {
		if (bufOutput.position() > 0) {
			bufOutput.flip();
			fcOutput.write(bufOutput);
			bufOutput.clear();
			//log.debug("offsetOutputUncommited=" + offsetOutputUncommited + " offsetOutputCommited=" + offsetOutputCommited + " fcOutput.position()=" + fcOutput.position());
			offsetOutputUncommited = offsetOutputCommited = fcOutput.position();
			if (syncOnFlush) {
				fcOutput.force(false);
				if (callback != null)
					callback.synched(offsetOutputCommited);
			}
		}
	}

	/**
	 * Forces any updates to this file to be written to the storage device that contains it.
	 * @return false if exception occur 
	 */
	public synchronized boolean sync() {
		if (!validState) throw new InvalidStateException();
		try { 
			flushBuffer();
			if (!syncOnFlush) {
				fcOutput.force(false);
				if (callback != null)
					callback.synched(offsetOutputCommited);
			}
			return true;
		}
		catch(Exception e) {
			log.error("Exception in sync()", e);
		}
		return false;
	}

	public static interface CallbackSync {
		public void synched(final long offset);
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
