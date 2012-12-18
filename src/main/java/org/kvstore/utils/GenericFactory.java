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
package org.kvstore.utils;
import java.lang.reflect.Array;

/**
 * Workarround for Java generics Reference: <a href=
 * "http://stackoverflow.com/questions/3731612/generic-array-creation-vs-array-newinstance"
 * >Generic array-creation</a>
 * 
 * @author Guillermo Grandes / guillermo.grandes[at]gmail.com
 */
public class GenericFactory<T> {
	private final Class<T> type;

	public GenericFactory(final Class<T> type) {
		this.type = type;
	}

	/**
	 * Allocate a new array of objects of type T.
	 * @param size of the array to allocate
	 * @return array allocated
	 */
	@SuppressWarnings({"unchecked"})
	public T[] newArray(final int size) {
		return (T[])Array.newInstance(type, size);
	}

	/**
	 * Create a new instance of type T
	 * @return new object of type T
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 */
	public T newInstance() throws InstantiationException, IllegalAccessException {
		return type.newInstance();
	}
}
