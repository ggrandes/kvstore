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
package org.kvstore.holders;
import java.nio.ByteBuffer;

import org.kvstore.structures.hash.FixedIntHashMap;

/**
 * Holder for Byte values
 *
 * @author Guillermo Grandes / guillermo.grandes[at]gmail.com
 */
public final class ByteHolder extends DataHolder<ByteHolder> {
	//
	private static final FixedIntHashMap<ByteHolder> cache = new FixedIntHashMap<ByteHolder>(4096, ByteHolder.class);
	//
	private final byte value;
	//
	public static ByteHolder valueOf(final byte value) {
		final ByteHolder cachedKey = cache.get(value);
		if (cachedKey != null) {
			if (cachedKey.value == value) {
				return cachedKey;
			}
		}
		final ByteHolder newKey = new ByteHolder(value);
		cache.put(value, newKey);
		return newKey;
	}

	/**
	 * Constructor necesario para la deserializacion
	 */
	public ByteHolder() { this((byte)0); };

	private ByteHolder(final byte value) {
		this.value = value;
	}

	public byte byteValue() {
		return value;
	}

	// ========= Basic Object methods =========

	public String toString() {
		return String.valueOf(value);
	}

	public int hashCode() {
		return (int)value;
	}

	// ========= Comparable =========

	public boolean equals(final Object obj) {
		if (obj instanceof ByteHolder) {
			return value == ((ByteHolder)obj).byteValue();
		}
		return false;
	}

	public int compareTo(final ByteHolder another) {
		final byte thisVal = this.value;
		final byte anotherVal = another.value;
		return ((thisVal<anotherVal) ? -1 : ((thisVal==anotherVal) ? 0 : 1));
	}

	// ========= Serialization =========

	public final int byteLength() {
		return 1;
	}

	public void serialize(ByteBuffer buf) {
		buf.put(value);
	}
	public ByteHolder deserialize(ByteBuffer buf) {
		return valueOf(buf.get());
	}

}

