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
package org.javastack.kvstore;

import java.util.Iterator;

import org.javastack.kvstore.holders.IntHolder;
import org.javastack.kvstore.structures.btree.BplusTreeFile;
import org.javastack.kvstore.structures.btree.BplusTree.InvalidDataException;
import org.javastack.kvstore.structures.btree.BplusTree.TreeEntry;

/**
 * Code example
 * 
 * @author Guillermo Grandes / guillermo.grandes[at]gmail.com
 */
public class Example {
	private static final String btreeFile = "/tmp/test";

	//
	public static void main(final String[] args) throws Exception {
		final int[] keys = new int[] { 5, 7, -11, 111, 0 };
		//
		KVStoreFactory<IntHolder, IntHolder> factory = new KVStoreFactory<IntHolder, IntHolder>(
				IntHolder.class, IntHolder.class);
		Options opts = factory.createTreeOptionsDefault()
				.set(KVStoreFactory.FILENAME, btreeFile);
		BplusTreeFile<IntHolder, IntHolder> tree = factory.createTreeFile(opts);
		//
		// Open & Recovery tree if needed
		try {
			if (tree.open())
				System.out.println("open tree ok");
		} catch (InvalidDataException e) {
			System.out.println("open tree error, recovery needed");
			if (tree.recovery(false) && tree.open()) {
				System.out.println("recovery ok, tree opened");
			} else {
				throw new Exception("Fatal Error Opening Tree");
			}
		}
		// clear all previous content if any
		tree.clear();
		// ============== PUT
		for (int i = 0; i < keys.length; i++) {
			final IntHolder key = IntHolder.valueOf(keys[i]);
			final IntHolder value = IntHolder.valueOf(i);
			tree.put(key, value);
		}
		tree.sync();
		// ============== GET
		System.out.println("tree.get(7)=" + tree.get(IntHolder.valueOf(7)));
		// ============== REMOVE
		tree.remove(IntHolder.valueOf(7));
		// ============== ITERATOR
		for (Iterator<TreeEntry<IntHolder, IntHolder>> i = tree.iterator(); i
				.hasNext();) {
			TreeEntry<IntHolder, IntHolder> e = i.next();
			System.out.println("Key=" + e.getKey() + " Value=" + e.getValue());
		}
		// ============== FIRST / LAST
		System.out.println("tree.firstKey()=" + tree.firstKey());
		System.out.println("tree.lastKey()=" + tree.lastKey());
		//
		tree.sync();
		tree.close();
	}

}
