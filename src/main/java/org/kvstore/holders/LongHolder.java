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
 * Holder for Long values
 * 
 * @author Guillermo Grandes / guillermo.grandes[at]gmail.com
 */
public final class LongHolder extends DataHolder<LongHolder> {
	private static final FixedIntHashMap<LongHolder> cache = new FixedIntHashMap<LongHolder>(8192,
			LongHolder.class);

	private final long value;

	public static LongHolder valueOf(final long value) {
		final int hash = (int) (value ^ (value >>> 32));
		final LongHolder cachedValue = cache.get(hash);
		if (cachedValue != null) {
			if (cachedValue.value == value) {
				return cachedValue;
			}
		}
		final LongHolder newValue = new LongHolder(value);
		cache.put(hash, newValue);
		return newValue;
	}

	/**
	 * Constructor necesario para la deserializacion
	 */
	public LongHolder() {
		this(0);
	};

	private LongHolder(final long value) {
		this.value = value;
	}

	public long longValue() {
		return (long) value;
	}

	// ========= Basic Object methods =========

	@Override
	public String toString() {
		return String.valueOf(value);
	}

	@Override
	public int hashCode() {
		return (int) (value ^ (value >>> 32));
	}

	// ========= Comparable =========

	@Override
	public boolean equals(final Object obj) {
		if (obj instanceof LongHolder) {
			return value == ((LongHolder) obj).longValue();
		}
		return false;
	}

	@Override
	public int compareTo(final LongHolder anotherLong) {
		final long thisVal = this.value;
		final long anotherVal = anotherLong.value;
		return ((thisVal < anotherVal) ? -1 : ((thisVal == anotherVal) ? 0 : 1));
	}

	// ========= Serialization =========

	@Override
	public final int byteLength() {
		return 8;
	}

	@Override
	public void serialize(final ByteBuffer buf) {
		buf.putLong(value);
	}

	@Override
	public LongHolder deserialize(final ByteBuffer buf) {
		return valueOf(buf.getLong());
	}

}
