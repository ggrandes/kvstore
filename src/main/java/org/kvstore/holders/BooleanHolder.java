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

/**
 * Holder for Boolean values
 *
 * @author Guillermo Grandes / guillermo.grandes[at]gmail.com
 */
public final class BooleanHolder extends DataHolder<BooleanHolder> {
	public static final BooleanHolder TRUE = new BooleanHolder(true);
	public static final BooleanHolder FALSE = new BooleanHolder(false);
	//
	private final boolean value;
	//
	public static BooleanHolder valueOf(final boolean value) {
		return (value ? TRUE : FALSE);
	}

	/**
	 * Constructor necesario para la deserializacion
	 */
	public BooleanHolder() { this(false); };

	private BooleanHolder(final boolean value) {
		this.value = value;
	}

	public boolean booleanValue() {
		return value;
	}

	// ========= Basic Object methods =========

	public String toString() {
		return (value ? "TRUE" : "FALSE");
	}

	public int hashCode() {
		return (value ? 1231 : 1237);
	}

	// ========= Comparable =========

	public boolean equals(final Object obj) {
		if (obj instanceof BooleanHolder) {
			return value == ((BooleanHolder)obj).value;
		}
		return false;
	}

	public int compareTo(final BooleanHolder another) {
		return (another.value == value ? 0 : (value ? 1 : -1));
	}

	// ========= Serialization =========

	public final int byteLength() {
		return 1;
	}

	public void serialize(ByteBuffer buf) {
		buf.put((byte)(value ? 1 : 0));
	}
	public BooleanHolder deserialize(ByteBuffer buf) {
		return valueOf((buf.get() == 0) ? false : true);
	}

}

