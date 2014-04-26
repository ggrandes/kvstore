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
package org.kvstore.test;

import java.nio.ByteBuffer;

import org.kvstore.io.FileBlockStore;
import org.kvstore.io.FileBlockStore.WriteBuffer;
import org.kvstore.io.FileStreamStore;
import org.kvstore.io.StringSerializer;

/**
 * Code for benchmark
 * 
 * @author Guillermo Grandes / guillermo.grandes[at]gmail.com
 */
public class BenchMarkDiskStore {
	private static final int TOTAL = (int) 1e6, TRACE_LEN = 100000;
	private static final String TEST_STREAM_FILE = "/tmp/data/stream";
	private static final String TEST_BLOCK_FILE = "/tmp/data/block";
	//
	public long long1;
	public long long2;
	public long long3;
	public int int1;
	public int int2;
	public String str1;
	public String str2;
	public String str3;
	public String str4;

	//
	public BenchMarkDiskStore() {
	}

	public BenchMarkDiskStore(final long long1, final long long2, final long long3, final int int1,
			final int int2, final String str1, final String str2, final String str3, final String str4) {
		this.long1 = long1;
		this.long2 = long2;
		this.long3 = long3;
		this.int1 = int1;
		this.int2 = int2;
		this.str1 = str1;
		this.str2 = str2;
		this.str3 = str3;
		this.str4 = str4;
	}

	// ByteBuffer
	public void serialize(final ByteBuffer out) {
		out.clear();
		out.putLong(long1);
		out.putLong(long2);
		out.putLong(long3);
		out.putInt(int1);
		out.putInt(int2);
		StringSerializer.fromStringToBuffer(out, str1);
		StringSerializer.fromStringToBuffer(out, str2);
		StringSerializer.fromStringToBuffer(out, str3);
		StringSerializer.fromStringToBuffer(out, str4);
	}

	public void deserialize(final ByteBuffer in) {
		long1 = in.getLong();
		long2 = in.getLong();
		long3 = in.getLong();
		int1 = in.getInt();
		int2 = in.getInt();
		str1 = StringSerializer.fromBufferToString(in);
		str2 = StringSerializer.fromBufferToString(in);
		str3 = StringSerializer.fromBufferToString(in);
		str4 = StringSerializer.fromBufferToString(in);
	}

	private static int c = 0;

	private final static BenchMarkDiskStore newData() {
		final String s1 = "S1.123456789.", s2 = "S2.123456789.", s3 = "S3.123456789.123456789.";
		final String s4 = "S4.123456789.123456789.123456789.123456789.123456789.123456789.123456789.123456789.123456789.123456789.123456789.123456789.123456789.123456789.123456789.123456";
		c++;
		return new BenchMarkDiskStore(c + 1, c + 2, c + 3, c + 11, c + 12, s1, s2, s3, s4);
	}

	public static void doTest_FileStreamStore_Bench_WriteRead() throws Exception {
		final int BUFFER_LEN = 0x10000;
		final FileStreamStore fss = new FileStreamStore(TEST_STREAM_FILE, BUFFER_LEN);
		final ByteBuffer buf = ByteBuffer.allocate(BUFFER_LEN);
		final long[] offset = new long[TOTAL];
		long ts, ts2;
		long newOffset;
		//
		fss.delete();
		fss.open();
		// Parameters
		fss.setFlushOnWrite(false);
		fss.setSyncOnFlush(false);
		fss.setAlignBlocks(true);
		// puts
		ts = System.currentTimeMillis();
		ts2 = ts;
		for (int i = 0; i < TOTAL; i++) {
			buf.clear();
			newData().serialize(buf);
			buf.flip();
			offset[i] = fss.write(buf);
			if (((i + 1) % TRACE_LEN) == 0) {
				System.out.println("offset[" + i + "]=" + offset[i] + "\t"
						+ (System.currentTimeMillis() - ts2) + "ms\t"
						+ (TRACE_LEN / Math.max((System.currentTimeMillis() - ts2), 1)) + "k/s");
				ts2 = System.currentTimeMillis();
			}
		}
		System.out.println("registry length=" + buf.limit());
		//
		fss.sync();
		System.out.println("WRITE: " + (System.currentTimeMillis() - ts) + "\t"
				+ (TOTAL / Math.max((System.currentTimeMillis() - ts), 1)) + "k/s");
		//
		// gets
		final BenchMarkDiskStore bag = new BenchMarkDiskStore();
		ts = System.currentTimeMillis();
		ts2 = ts;
		newOffset = 0;
		for (int j = 0; j < offset.length; j++) {
			final long i = newOffset;
			buf.clear();
			newOffset = fss.read(newOffset, buf);
			if (newOffset < 0) {
				System.out.println("Error trying read offset " + i + " size=" + fss.size());
				break;
			}
			bag.deserialize(buf);
			if (((j + 1) % TRACE_LEN) == 0) {
				System.out.println("offset=[" + i + "] newOffset=[" + newOffset + "]" + "\t"
						+ (System.currentTimeMillis() - ts2) + "ms\t"
						+ (TRACE_LEN / Math.max((System.currentTimeMillis() - ts2), 1)) + "k/s");
				ts2 = System.currentTimeMillis();
			}
		}
		System.out.println("READ: " + (System.currentTimeMillis() - ts) + "\t"
				+ (TOTAL / Math.max((System.currentTimeMillis() - ts), 1)) + "k/s");
		//
		fss.close();
		fss.delete();
	}

	public static void doTest_FileBlockStore_Bench_WriteRead() throws Exception {
		final int BLOCK_SIZE = 512;
		final FileBlockStore fbs = new FileBlockStore(TEST_BLOCK_FILE, BLOCK_SIZE, false);
		long ts, ts2;
		//
		fbs.delete();
		// fbs.enableMmap(); // Test MMAPED?
		fbs.open();
		// puts
		ts = System.currentTimeMillis();
		ts2 = ts;
		for (int i = 0; i < (TOTAL / 2); i++) {
			final WriteBuffer wbuf = fbs.set(i);
			final ByteBuffer buf = wbuf.buf();
			newData().serialize(buf);
			newData().serialize(buf);
			buf.flip();
			wbuf.save();
			if (((i + 1) % TRACE_LEN) == 0) {
				System.out.println("block[" + i + "]" + "\t" + (System.currentTimeMillis() - ts2) + "ms\t"
						+ (TRACE_LEN / Math.max((System.currentTimeMillis() - ts2), 1)) + "k/s");
				ts2 = System.currentTimeMillis();
			}
		}
		//
		fbs.sync();
		System.out.println("WRITE: " + (System.currentTimeMillis() - ts) + "\t"
				+ (TOTAL / Math.max((System.currentTimeMillis() - ts), 1)) + "k/s");
		//
		// gets
		final BenchMarkDiskStore bag = new BenchMarkDiskStore();
		ts = System.currentTimeMillis();
		ts2 = ts;
		for (int j = 0; j < TOTAL; j++) {
			final ByteBuffer buf = fbs.get(j);
			if (buf == null) {
				System.out.println("Error trying read block " + j + " blocks=" + fbs.sizeInBlocks());
				break;
			}
			bag.deserialize(buf);
			bag.deserialize(buf);
			if (((j + 1) % TRACE_LEN) == 0) {
				System.out.println("block=[" + j + "]" + "\t" + (System.currentTimeMillis() - ts2) + "ms\t"
						+ (TRACE_LEN / Math.max((System.currentTimeMillis() - ts2), 1)) + "k/s");
				ts2 = System.currentTimeMillis();
			}
		}
		System.out.println("READ: " + (System.currentTimeMillis() - ts) + "\t"
				+ (TOTAL / Math.max((System.currentTimeMillis() - ts), 1)) + "k/s");
		//
		fbs.close();
		fbs.delete();
	}

	public static void main(final String[] args) throws Exception {
		System.out.println("------- BEGIN TEST -------");
		System.out.println("------- Stream -------");
		doTest_FileStreamStore_Bench_WriteRead();
		System.out.println("------- Block -------");
		doTest_FileBlockStore_Bench_WriteRead();
		System.out.println("------- END TEST -------");
	}
}
