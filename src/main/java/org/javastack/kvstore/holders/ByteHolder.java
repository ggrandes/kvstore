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
package org.javastack.kvstore.holders;

import java.nio.ByteBuffer;

/**
 * Holder for Byte values
 * 
 * @author Guillermo Grandes / guillermo.grandes[at]gmail.com
 */
public final class ByteHolder extends DataHolder<ByteHolder> {
	private static final ByteHolder[] cache = new ByteHolder[256];

	static {
		for (int i = 0; i < cache.length; i++) {
			cache[i] = new ByteHolder((byte) (i - 128));
		}
	}

	private final byte value;

	public static ByteHolder valueOf(final byte value) {
		return cache[(int) value + 128];
	}

	/**
	 * Constructor necesario para la deserializacion
	 */
	public ByteHolder() {
		this((byte) 0);
	};

	private ByteHolder(final byte value) {
		this.value = value;
	}

	public byte byteValue() {
		return value;
	}

	// ========= Bit Operation methods =========

	/**
	 * MSB-left (76543210), 0x7F=127=01111111
	 * 
	 * @param bitIndex to check
	 * @return true=1, false=0
	 */
	public boolean get(final int bitIndex) {
		if (bitIndex > 7 || bitIndex < 0)
			throw new IndexOutOfBoundsException();
		return (((((int) value) >> bitIndex) & 1) == 1);
	}

	/**
	 * MSB-left (76543210), 0x7F=127=01111111
	 * 
	 * @param bitIndex to set
	 * @return new ByteHolder with bits updated
	 */
	public ByteHolder set(final int bitIndex) {
		if (bitIndex > 7 || bitIndex < 0)
			throw new IndexOutOfBoundsException();
		return valueOf((byte) (value | (1 << bitIndex)));
	}

	/**
	 * MSB-left (76543210), 0x7F=127=01111111
	 * 
	 * @param bitIndex to clear
	 * @return new ByteHolder with bits updated
	 */
	public ByteHolder clear(final int bitIndex) {
		if (bitIndex > 7 || bitIndex < 0)
			throw new IndexOutOfBoundsException();
		return valueOf((byte) (value & ~(1 << bitIndex)));
	}

	// ========= Basic Object methods =========

	@Override
	public String toString() {
		return String.valueOf(value);
	}

	@Override
	public int hashCode() {
		return (int) value;
	}

	// ========= Comparable =========

	@Override
	public boolean equals(final Object obj) {
		if (obj instanceof ByteHolder) {
			return value == ((ByteHolder) obj).byteValue();
		}
		return false;
	}

	@Override
	public int compareTo(final ByteHolder another) {
		final byte thisVal = this.value;
		final byte anotherVal = another.value;
		return ((thisVal < anotherVal) ? -1 : ((thisVal == anotherVal) ? 0 : 1));
	}

	// ========= Serialization =========

	@Override
	public final int byteLength() {
		return 1;
	}

	@Override
	public void serialize(final ByteBuffer buf) {
		buf.put(value);
	}

	@Override
	public ByteHolder deserialize(final ByteBuffer buf) {
		return valueOf(buf.get());
	}
}
