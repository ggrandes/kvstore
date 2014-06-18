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
 * Holder for Int values
 * 
 * @author Guillermo Grandes / guillermo.grandes[at]gmail.com
 */
public final class IntHolder extends DataHolder<IntHolder> {
	private static final FixedIntHashMap<IntHolder> cache = new FixedIntHashMap<IntHolder>(4096,
			IntHolder.class);

	private final int value;

	public static IntHolder valueOf(final long value) {
		return valueOf((int) value);
	}

	public static IntHolder valueOf(final int value) {
		final IntHolder cachedKey = cache.get(value);
		if (cachedKey != null) {
			if (cachedKey.value == value) {
				return cachedKey;
			}
		}
		final IntHolder newKey = new IntHolder(value);
		cache.put(value, newKey);
		return newKey;
	}

	/**
	 * Constructor necesario para la deserializacion
	 */
	public IntHolder() {
		this(0);
	};

	private IntHolder(final int value) {
		this.value = value;
	}

	public int intValue() {
		return value;
	}

	// ========= Basic Object methods =========

	@Override
	public String toString() {
		return String.valueOf(value);
	}

	@Override
	public int hashCode() {
		return value;
	}

	// ========= Comparable =========

	@Override
	public boolean equals(final Object obj) {
		if (obj instanceof IntHolder) {
			return value == ((IntHolder) obj).intValue();
		}
		return false;
	}

	@Override
	public int compareTo(final IntHolder anotherInt) {
		final int thisVal = this.value;
		final int anotherVal = anotherInt.value;
		return ((thisVal < anotherVal) ? -1 : ((thisVal == anotherVal) ? 0 : 1));
	}

	// ========= Serialization =========

	@Override
	public final int byteLength() {
		return 4;
	}

	@Override
	public void serialize(final ByteBuffer buf) {
		buf.putInt(value);
	}

	@Override
	public IntHolder deserialize(final ByteBuffer buf) {
		return valueOf(buf.getInt());
	}

}
