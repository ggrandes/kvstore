# KVStore

KVStore is an open source (Apache License, Version 2.0) Java Key-Value Store (memory & disk). Do not require any external lib.

### Currently in Development

---

# Usage Example

```java
		final BplusTreeFile<IntHolder, IntHolder> tree = new BplusTreeFile<IntHolder, IntHolder>(true, 512, IntHolder.class, IntHolder.class, btreeFile);
		//
		// Clear all data in datafiles
		tree.clear();
		// ============== INSERT
		for (int i = 0; i < keys.length; i++) {
			final IntHolder key = IntHolder.valueOf(keys[i]);
			final IntHolder value = IntHolder.valueOf(i);
			tree.put(key, value);
		}
		// Force write changes to disk
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
		// Force write changes to disk
		tree.sync();
		// Close
		tree.close();
```


---
Inspired in [Book: Open Data Structures in Java](http://opendatastructures.org/ods-java/14_2_B_Trees.html), [Perl DB_File](http://search.cpan.org/~pmqs/DB_File-1.827/DB_File.pm), [JDBM3](https://github.com/jankotek/JDBM3) and [H2-Database](http://www.h2database.com/), this code is Java-minimalistic version.
