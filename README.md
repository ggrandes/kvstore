# KVStore

KVStore is an open source (Apache License, Version 2.0) Java "fixed length" Key-Value Store (memory & disk).
API is similar to [TreeMap](http://docs.oracle.com/javase/6/docs/api/java/util/TreeMap.html)

### Currently in Development

---

# Usage Example

```java
import java.util.Iterator;
import org.kvstore.holders.IntHolder;
import org.kvstore.structures.btree.BplusTree.TreeEntry;
import org.kvstore.structures.btree.BplusTreeFile;

public class Example {
	private static final String btreeFile = "/tmp/test";
	//
	public static void main(final String[] args) throws Exception {
		final int[] keys = new int[] {
				5, 7, -11, 111, 0
		};
		//
		BplusTreeFile<IntHolder, IntHolder> tree;
		tree = new BplusTreeFile<IntHolder, IntHolder>(
			true, 512, 
			IntHolder.class, IntHolder.class, 
			btreeFile
		);
		//
		tree.clear();
		// ============== INSERT
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
		for (Iterator<TreeEntry<IntHolder, IntHolder>> i = tree.iterator(); i.hasNext();) {
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
```

#### The Result:

	tree.get(7)=1
	Key=-11 Value=2
	Key=0 Value=4
	Key=5 Value=0
	Key=111 Value=3
	tree.firstKey()=-11
	tree.lastKey()=111


---
Inspired in [Book: Open Data Structures in Java](http://opendatastructures.org/ods-java/14_2_B_Trees.html), [Perl DB_File](http://search.cpan.org/~pmqs/DB_File-1.827/DB_File.pm), [JDBM3](https://github.com/jankotek/JDBM3) and [H2-Database](http://www.h2database.com/), this code is Java-minimalistic version.
