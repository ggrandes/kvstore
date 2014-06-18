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
package org.javastack.kvstore.test;

import java.nio.ByteBuffer;

import org.javastack.kvstore.holders.DataHolder;
import org.javastack.kvstore.holders.IntHolder;
import org.javastack.kvstore.io.StringSerializer;
import org.javastack.kvstore.structures.btree.BplusTreeMemory;
import org.javastack.kvstore.structures.hash.IntHashMap;
import org.javastack.kvstore.structures.hash.IntLinkedHashMap;

/**
 * Code for benchmark
 *
 * @author Guillermo Grandes / guillermo.grandes[at]gmail.com
 */
public class BenchMarkMemoryStructures extends DataHolder<BenchMarkMemoryStructures> {
	private static final int TOTAL = (int)2e6, TRACE_LEN = 100000;
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
	public BenchMarkMemoryStructures() {}
	public BenchMarkMemoryStructures(final long long1, final long long2, final long long3, 
			final int int1, final int int2, 
			final String str1, final String str2, final String str3, final String str4) {
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
	public BenchMarkMemoryStructures deserialize(final ByteBuffer in) {
		long1 = in.getLong();
		long2 = in.getLong();
		long3 = in.getLong();
		int1 = in.getInt();
		int2 = in.getInt();
		str1 = StringSerializer.fromBufferToString(in);
		str2 = StringSerializer.fromBufferToString(in);
		str3 = StringSerializer.fromBufferToString(in);
		str4 = StringSerializer.fromBufferToString(in);
		return new BenchMarkMemoryStructures(long1, long2, long3, int1, int2, str1, str2, str3, str4);
	}

	private static int c = 0;
	private final static BenchMarkMemoryStructures newData() {
		final String s1 = "S1.123456789.", s2 = "S2.123456789.", s3 = "S3.123456789.123456789.";
		final String s4 = "S4.123456789.123456789.123456789.123456789.123456789.123456789.123456789.123456789.123456789.123456789.123456789.123456789.123456789.123456789.123456789.123456";
		c++;
		return new BenchMarkMemoryStructures(c+1, c+2, c+3, c+11, c+12, s1, s2, s3, s4);
	}
	public static void doTest_TreeMemory_Bench_PutGetRemove() throws Exception {
		final BplusTreeMemory<IntHolder, BenchMarkMemoryStructures> tree = new BplusTreeMemory<IntHolder, BenchMarkMemoryStructures>(511, IntHolder.class, BenchMarkMemoryStructures.class);
		long ts, ts2;
		//
		// puts
		ts = System.currentTimeMillis(); ts2 = ts;
		for (int i = 0; i < TOTAL; i++) {
			tree.put(IntHolder.valueOf(i), newData());
			if (((i+1) % TRACE_LEN) == 0) {
				System.out.println("put["+i+"]" + "\t" + (System.currentTimeMillis() - ts2) + "ms\t" + (TRACE_LEN / Math.max((System.currentTimeMillis() - ts2), 1)) + "k/s");
                ts2 = System.currentTimeMillis();
			}
		}
		//
        System.out.println("PUT: " + (System.currentTimeMillis() - ts) + "\t" + (TOTAL / Math.max((System.currentTimeMillis() - ts), 1)) + "k/s");
        //
		// gets
		ts = System.currentTimeMillis(); ts2 = ts;
		for (int j = 0; j < TOTAL; j++) {
			final BenchMarkMemoryStructures bag = tree.get(IntHolder.valueOf(j));
			if (bag == null) { 
				System.out.println("Error trying get(" + j + ")");
				break;
			}
			if (((j+1) % TRACE_LEN) == 0) {
				System.out.println("get=["+j+"]"+ "\t" + (System.currentTimeMillis() - ts2) + "ms\t" + (TRACE_LEN / Math.max((System.currentTimeMillis() - ts2), 1)) + "k/s");
                ts2 = System.currentTimeMillis();
			}
		}
        System.out.println("GET: " + (System.currentTimeMillis() - ts) + "\t" + (TOTAL / Math.max((System.currentTimeMillis() - ts), 1)) + "k/s");
		//
		// remove
		ts = System.currentTimeMillis(); ts2 = ts;
		for (int i = 0; i < TOTAL; i++) {
			tree.remove(IntHolder.valueOf(i));
			if (((i+1) % TRACE_LEN) == 0) {
				System.out.println("remove["+i+"]" + "\t" + (System.currentTimeMillis() - ts2) + "ms\t" + (TRACE_LEN / Math.max((System.currentTimeMillis() - ts2), 1)) + "k/s");
                ts2 = System.currentTimeMillis();
			}
		}
		//
        System.out.println("REMOVE: " + (System.currentTimeMillis() - ts) + "\t" + (TOTAL / Math.max((System.currentTimeMillis() - ts), 1)) + "k/s");
		//
	}

	public static void doTest_IntHashMemory_Bench_PutGetRemove() throws Exception {
		final IntHashMap<BenchMarkMemoryStructures> hash = new IntHashMap<BenchMarkMemoryStructures>(TOTAL * 2, BenchMarkMemoryStructures.class);
		long ts, ts2;
		//
		// puts
		ts = System.currentTimeMillis(); ts2 = ts;
		for (int i = 0; i < TOTAL; i++) {
			hash.put(i, newData());
			if (((i+1) % TRACE_LEN) == 0) {
				System.out.println("put["+i+"]" + "\t" + (System.currentTimeMillis() - ts2) + "ms\t" + (TRACE_LEN / Math.max((System.currentTimeMillis() - ts2), 1)) + "k/s");
                ts2 = System.currentTimeMillis();
			}
		}
		//
        System.out.println("PUT: " + (System.currentTimeMillis() - ts) + "\t" + (TOTAL / Math.max((System.currentTimeMillis() - ts), 1)) + "k/s");
        //
		// gets
		ts = System.currentTimeMillis(); ts2 = ts;
		for (int j = 0; j < TOTAL; j++) {
			final BenchMarkMemoryStructures bag = hash.get(j);
			if (bag == null) { 
				System.out.println("Error trying get(" + j + ")");
				break;
			}
			if (((j+1) % TRACE_LEN) == 0) {
				System.out.println("get=["+j+"]"+ "\t" + (System.currentTimeMillis() - ts2) + "ms\t" + (TRACE_LEN / Math.max((System.currentTimeMillis() - ts2), 1)) + "k/s");
                ts2 = System.currentTimeMillis();
			}
		}
        System.out.println("GET: " + (System.currentTimeMillis() - ts) + "\t" + (TOTAL / Math.max((System.currentTimeMillis() - ts), 1)) + "k/s");
		//
		// remove
		ts = System.currentTimeMillis(); ts2 = ts;
		for (int i = 0; i < TOTAL; i++) {
			hash.remove(i);
			if (((i+1) % TRACE_LEN) == 0) {
				System.out.println("remove["+i+"]" + "\t" + (System.currentTimeMillis() - ts2) + "ms\t" + (TRACE_LEN / Math.max((System.currentTimeMillis() - ts2), 1)) + "k/s");
                ts2 = System.currentTimeMillis();
			}
		}
		//
        System.out.println("REMOVE: " + (System.currentTimeMillis() - ts) + "\t" + (TOTAL / Math.max((System.currentTimeMillis() - ts), 1)) + "k/s");
		//
	}
	public static void doTest_IntLinkedHashMemory_Bench_PutGetRemove() throws Exception {
		final IntLinkedHashMap<BenchMarkMemoryStructures> hash = new IntLinkedHashMap<BenchMarkMemoryStructures>(TOTAL * 2, BenchMarkMemoryStructures.class);
		long ts, ts2;
		//
		// puts
		ts = System.currentTimeMillis(); ts2 = ts;
		for (int i = 0; i < TOTAL; i++) {
			hash.put(i, newData());
			if (((i+1) % TRACE_LEN) == 0) {
				System.out.println("put["+i+"]" + "\t" + (System.currentTimeMillis() - ts2) + "ms\t" + (TRACE_LEN / Math.max((System.currentTimeMillis() - ts2), 1)) + "k/s");
                ts2 = System.currentTimeMillis();
			}
		}
		//
        System.out.println("PUT: " + (System.currentTimeMillis() - ts) + "\t" + (TOTAL / Math.max((System.currentTimeMillis() - ts), 1)) + "k/s");
        //
		// gets
		ts = System.currentTimeMillis(); ts2 = ts;
		for (int j = 0; j < TOTAL; j++) {
			final BenchMarkMemoryStructures bag = hash.get(j);
			if (bag == null) { 
				System.out.println("Error trying get(" + j + ")");
				break;
			}
			if (((j+1) % TRACE_LEN) == 0) {
				System.out.println("get=["+j+"]"+ "\t" + (System.currentTimeMillis() - ts2) + "ms\t" + (TRACE_LEN / Math.max((System.currentTimeMillis() - ts2), 1)) + "k/s");
                ts2 = System.currentTimeMillis();
			}
		}
        System.out.println("GET: " + (System.currentTimeMillis() - ts) + "\t" + (TOTAL / Math.max((System.currentTimeMillis() - ts), 1)) + "k/s");
		//
		// remove
		ts = System.currentTimeMillis(); ts2 = ts;
		for (int i = 0; i < TOTAL; i++) {
			hash.remove(i);
			if (((i+1) % TRACE_LEN) == 0) {
				System.out.println("remove["+i+"]" + "\t" + (System.currentTimeMillis() - ts2) + "ms\t" + (TRACE_LEN / Math.max((System.currentTimeMillis() - ts2), 1)) + "k/s");
                ts2 = System.currentTimeMillis();
			}
		}
		//
        System.out.println("REMOVE: " + (System.currentTimeMillis() - ts) + "\t" + (TOTAL / Math.max((System.currentTimeMillis() - ts), 1)) + "k/s");
		//
	}
	// Not required for memory tree
	@Override
	public String toString() {
		throw new UnsupportedOperationException();
	}
	@Override
	public int hashCode() {
		throw new UnsupportedOperationException();
	}
	@Override
	public boolean equals(Object obj) {
		throw new UnsupportedOperationException();
	}
	@Override
	public int compareTo(BenchMarkMemoryStructures another) {
		throw new UnsupportedOperationException();
	}
	@Override
	public int byteLength() {
		throw new UnsupportedOperationException();
	}

	public static void main(final String[] args) throws Exception {
		System.out.println("------- BEGIN TEST -------");
		System.out.println("------- B+Tree -------");
		doTest_TreeMemory_Bench_PutGetRemove();
		System.out.println("------- IntHashMap -------");
		doTest_IntHashMemory_Bench_PutGetRemove();
		System.out.println("------- IntLinkedHashMap -------");
		doTest_IntLinkedHashMemory_Bench_PutGetRemove();
		System.out.println("------- END TEST -------");
	}
}
