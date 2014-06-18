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
 * Basic Holder for data (int, long,...)
 * 
 * @param <T>
 * 
 * @author Guillermo Grandes / guillermo.grandes[at]gmail.com
 */
public abstract class DataHolder<T> implements Comparable<T>, HolderSerializable<T> {

	// ========= Basic Object methods =========

	@Override
	abstract public String toString();

	@Override
	abstract public int hashCode();

	// ========= Comparable =========

	@Override
	abstract public boolean equals(final Object obj);

	@Override
	abstract public int compareTo(final T another);

	// ========= Serialization =========

	@Override
	abstract public int byteLength();

	@Override
	abstract public void serialize(final ByteBuffer buf);

	@Override
	abstract public T deserialize(final ByteBuffer buf);

}
