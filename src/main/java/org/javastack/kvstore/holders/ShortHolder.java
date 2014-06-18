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

import org.javastack.kvstore.structures.hash.FixedIntHashMap;

/**
 * Holder for Short values
 * 
 * @author Guillermo Grandes / guillermo.grandes[at]gmail.com
 */
public final class ShortHolder extends DataHolder<ShortHolder> {
	private static final FixedIntHashMap<ShortHolder> cache = new FixedIntHashMap<ShortHolder>(4096,
			ShortHolder.class);

	private final short value;

	public static ShortHolder valueOf(final short value) {
		final ShortHolder cachedKey = cache.get(value);
		if (cachedKey != null) {
			if (cachedKey.value == value) {
				return cachedKey;
			}
		}
		final ShortHolder newKey = new ShortHolder(value);
		cache.put(value, newKey);
		return newKey;
	}

	/**
	 * Constructor necesario para la deserializacion
	 */
	public ShortHolder() {
		this((short) 0);
	};

	private ShortHolder(final short value) {
		this.value = value;
	}

	public short shortValue() {
		return value;
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
		if (obj instanceof ShortHolder) {
			return value == ((ShortHolder) obj).shortValue();
		}
		return false;
	}

	@Override
	public int compareTo(final ShortHolder another) {
		final short thisVal = this.value;
		final short anotherVal = another.value;
		return ((thisVal < anotherVal) ? -1 : ((thisVal == anotherVal) ? 0 : 1));
	}

	// ========= Serialization =========

	@Override
	public final int byteLength() {
		return 2;
	}

	@Override
	public void serialize(final ByteBuffer buf) {
		buf.putShort(value);
	}

	@Override
	public ShortHolder deserialize(final ByteBuffer buf) {
		return valueOf(buf.getShort());
	}

}
