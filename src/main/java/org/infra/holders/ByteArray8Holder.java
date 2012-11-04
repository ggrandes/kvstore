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
package org.infra.holders;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.infra.structures.hash.FixedIntHashMap;

/**
 * Example Holder for byte[8] values
 *
 * @author Guillermo Grandes / guillermo.grandes[at]gmail.com
 */
public final class ByteArray8Holder extends DataHolder<ByteArray8Holder> {
	//
	private static final FixedIntHashMap<ByteArray8Holder> cache = new FixedIntHashMap<ByteArray8Holder>(4096, ByteArray8Holder.class);

	private final byte[] value;

	public static ByteArray8Holder valueOf(final byte[] value) {
		final int hash = Arrays.hashCode(value);
		final ByteArray8Holder cachedKey = cache.get(hash);
		if (cachedKey != null) {
			if (cachedKey.value == value) {
				return cachedKey;
			}
		}
		final ByteArray8Holder newKey = new ByteArray8Holder(value);
		cache.put(hash, newKey);
		return newKey;
	}

	/**
	 * Constructor necesario para la deserializacion
	 */
	public ByteArray8Holder() { this(null); };

	private ByteArray8Holder(final byte[] value) {
		this.value = value;
	}

	public byte[] getValue() {
		return value;
	}

	// ========= Basic Object methods =========

	public String toString() {
		return getHex(value); // Arrays.toString(value);
	}

	public int hashCode() {
		return Arrays.hashCode(value); 
	}

	private static final String HEXES = "0123456789ABCDEF";
	public static String getHex( byte [] raw ) {
		if ( raw == null ) {
			return null;
		}
		final StringBuilder hex = new StringBuilder( 2 * raw.length );
		for ( final byte b : raw ) {
			hex
			.append(HEXES.charAt((b & 0xF0) >> 4))
			.append(HEXES.charAt((b & 0x0F)));
		}
		return hex.toString();
	}

	// ========= Comparable =========

	public boolean equals(final Object obj) {
		if (obj instanceof ByteArray8Holder) {
			return Arrays.equals(value, ((ByteArray8Holder)obj).getValue());
		}
		return false;
	}

	public int compareTo(final ByteArray8Holder anotherLong) {
		final byte[] thisVal = this.value;
		final byte[] anotherVal = anotherLong.value;
		return compare(thisVal, anotherVal);
	}

	private final static int compare(final byte[] left, final byte[] right) {
		for (int i = 0, j = 0; i < left.length && j < right.length; i++, j++) {
			int a = (left[i] & 0xff);
			int b = (right[j] & 0xff);
			if (a != b) {
				return a - b;
			}
		}
		return left.length - right.length;
	}


	// ========= Serialization =========

	public final int byteLength() {
		return 8;
	}

	public void serialize(ByteBuffer buf) {
		buf.put(value, 0, Math.min(byteLength(), value.length));
	}
	public ByteArray8Holder deserialize(ByteBuffer buf) {
		final byte[] newvalue = new byte[byteLength()];
		buf.get(newvalue, 0, newvalue.length);
		return valueOf(newvalue);
	}

	// ========= 

	public static void main(String[] args) {
		ByteArray8Holder b1 = new ByteArray8Holder(new byte[] { 0,0, 0,0, 0,0, 0,1 });
		ByteArray8Holder b2 = new ByteArray8Holder(new byte[] { 0,0, 0,0, 0,0, 0,2 });
		ByteArray8Holder b2b = new ByteArray8Holder(new byte[] { 0,0, 0,0, 0,0, 0,2 });
		System.out.println("compareTo=" + Integer.valueOf(0).compareTo(Integer.valueOf(1)));
		System.out.println("b1.compareTo(b2)=" + b1.compareTo(b2));
		System.out.println("b1.equals(b2)=" + b1.equals(b2));
		System.out.println("b2.equals(b2b)=" + b2.equals(b2b));
		System.out.println("b1.hashCode()=" + b1.hashCode());
		System.out.println("b2.hashCode()=" + b2.hashCode());
		System.out.println("b2b.hashCode()=" + b2b.hashCode());
		System.out.println("b2b.toString()=" + b2b.toString());
	}

}

