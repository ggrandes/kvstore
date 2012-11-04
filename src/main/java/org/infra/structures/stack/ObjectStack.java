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
package org.infra.structures.stack;
import java.util.ArrayDeque;

/**
 * Basic Stack using ArrayDeque
 * This class is NOT Thread-Safe
 * @param <T>
 *
 * @author Guillermo Grandes / guillermo.grandes[at]gmail.com
 */
public final class ObjectStack<T> {
	private ArrayDeque<T> deque;

	/**
	 * Constructs an empty stack with an initial capacity sufficient to 
	 * hold 8 elements.
	 */
	public ObjectStack() {
		this(8);
	}

	/**
	 * Constructs an empty stack with an initial capacity sufficient to 
	 * hold the specified number of elements.
	 * @param size lower bound on initial capacity of the stack
	 */
	public ObjectStack(final int size) {
		deque = new ArrayDeque<T>(size);
	}

	/**
	 * Removes all of the elements from this stack. 
	 * The stack will be empty after this call returns.
	 */
	public final void clear() { 
		if (deque.isEmpty()) return;
		deque.clear();
	}

	/**
	 * Returns true if this stack contains no elements.
	 * @return true if this stack contains no elements.
	 */
	public final boolean isEmpty() {
		return deque.isEmpty();
	}

	/**
	 * Retrieves and removes the last element of this stack, 
	 * or returns null if this stack is empty.
	 * @return the tail of this stack, or null if this stack is empty
	 */
	public final T pop() {
		return deque.pollLast();
	}

	/**
	 * Inserts the specified element at the end of this stack.
	 * @param e the element to add
	 */
	public final void push(final T e) {
		deque.offerLast(e);
	}

	/**
	 * Return the number of elements in stack
	 * @return int with number of elements
	 */
	public final int size() {
		return deque.size();
	}

}
