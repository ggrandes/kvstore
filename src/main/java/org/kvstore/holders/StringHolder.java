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
import java.nio.charset.Charset;

import org.kvstore.pool.StringPool;

/**
 * Holder for String values
 * <br/><b>WARNING:</b> Dont use this with BplusTreeFile (file need fixed/constant length objects, like Long or Int)
 *
 * @author Guillermo Grandes / guillermo.grandes[at]gmail.com
 */
public final class StringHolder extends DataHolder<StringHolder> {
	//
	private final String value;
	//	
	public static StringHolder valueOf(final String value) {
		return new StringHolder(StringPool.getCanonicalVersion(value));
	}

	/**
	 * Constructor necesario para la deserializacion
	 */
	public StringHolder() { this(""); };

	private StringHolder(final String value) {
		this.value = value;
	}

	public String getValue() {
		return value;
	}

	// ========= Basic Object methods =========

	public String toString() {
		return value;
	}

	public int hashCode() {
		return value.hashCode();
	}

	// ========= Comparable =========

	public boolean equals(final Object obj) {
		if (obj instanceof StringHolder) {
			return value.equals(obj);
		}
		return false;
	}

	public int compareTo(final StringHolder anotherString) {
		final String thisVal = this.value;
		final String anotherVal = anotherString.value;
		return thisVal.compareTo(anotherVal);
	}

	// ========= Serialization =========

	public final int byteLength() {
		throw new UnsupportedOperationException("StringHolder is variable length Object");
	}

	public void serialize(ByteBuffer buf) {
		fromStringToBuffer(buf, value);
	}

	public StringHolder deserialize(ByteBuffer buf) {
		return valueOf(fromBufferToString(buf));
	}

	// Helper
	private static final Charset cs = Charset.forName("UTF-8");
	public static final void fromStringToBuffer(final ByteBuffer out, final String str) {
		if (str == null) {
			out.putInt(Integer.MIN_VALUE);
			return;
		}
		final byte[] bytes = str.getBytes(cs);
		final int len = bytes.length;
		out.putInt(len);
		out.put(bytes, 0, len);
	}
	public static final String fromBufferToString(final ByteBuffer in) {
		final int len = in.getInt();
		if (len == Integer.MIN_VALUE) {
			return null;
		}
		final byte[] bytes = new byte[len];
		in.get(bytes, 0, len);
		return new String(bytes, 0, len, cs);
	}

}
