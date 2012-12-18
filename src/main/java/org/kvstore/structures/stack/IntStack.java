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
package org.kvstore.structures.stack;
import java.util.Arrays;

/**
 * Native Int Stack
 * This class is NOT Thread-Safe
 *
 * @author Guillermo Grandes / guillermo.grandes[at]gmail.com
 */
public class IntStack {
	private static final int DEFAULT_NULL = -1;
	private final int null_value;
	private int stackPointer;
	private int[] stack;

	/**
	 * Instantiate Native Int Stack of specified size and default null value (-1)
	 * @param size size of stack
	 */
	public IntStack(final int size) {
		this(size, DEFAULT_NULL);
	}

	/**
	 * Instantiate Native Int Stack of specified size and custom null value
	 * @param size size of stack
	 * @param null_value to return if stack is empty
	 */
	public IntStack(final int size, final int null_value) {
		this.stack = new int[size];
		this.null_value = null_value;
	}

	// Resize Stack
	private final void growToHold(final int length) {
		if (length > stack.length) {
			final int[] newStack = new int[Math.max(length, stack.length*2)];
			System.arraycopy(stack, 0, newStack, 0, stack.length);
			stack = newStack;
		}
	}

	/**
	 * Removes all of the elements from this stack. The stack will be empty after this call returns.
	 */
	public void clear() {
		if (isEmpty()) return;
		stackPointer = 0;
		Arrays.fill(stack, null_value);
	}

	/**
	 * Returns true if this stack contains no elements.
	 * @return true if this stack contains no elements.
	 */
	public boolean isEmpty() {
		return (stackPointer == 0);
	}

	/**
	 * Push value on top of stack
	 * @param value to push
	 */
	public final void push(final int value) {
		growToHold(stackPointer+1);
		stack[stackPointer++] = value;
	}

	/**
	 * Pop value from top of stack
	 * @return int of value
	 */
	public final int pop() {
		if (stackPointer == 0) {
			return null_value;
		}
		final int element = stack[--stackPointer];
		stack[stackPointer] = null_value;
		return element;
	}

	/**
	 * Return the number of elements in stack
	 * @return int with number of elements
	 */
	public final int size() {
		return stackPointer;
	}

	public static void main(String[] args) {
		IntStack stack = new IntStack(16, -1);
		System.out.println(stack.pop());
		stack.push(12);
		stack.push(99);
		stack.push(13);
		stack.push(98);
		System.out.println(stack.pop());
		System.out.println(stack.pop());
		System.out.println(stack.pop());
		System.out.println(stack.pop());
		System.out.println(stack.pop());
	}
}