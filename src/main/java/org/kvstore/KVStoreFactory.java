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
package org.kvstore;

import org.apache.log4j.Logger;
import org.kvstore.holders.DataHolder;
import org.kvstore.structures.btree.BplusTree;
import org.kvstore.structures.btree.BplusTreeFile;
import org.kvstore.structures.btree.BplusTreeMemory;

public class KVStoreFactory<K extends DataHolder<K>, V extends DataHolder<V>> {
	private static final Logger log = Logger.getLogger(KVStoreFactory.class);
	//
	public static final String FILENAME = "opt.kvstore.persistence.string.filename"; // String
	public static final String CACHE_SIZE = "opt.kvstore.persistence.int.cachesize"; // int
	public static final String AUTO_TUNE = "opt.kvstore.persistence.boolean.autotune"; // Boolean
	public static final String USE_REDO = "opt.kvstore.persistence.boolean.useredo"; // Boolean
	public static final String USE_REDO_THREAD = "opt.kvstore.persistence.boolean.useredothread"; // Boolean
	public static final String DISABLE_POPULATE_CACHE = "opt.kvstore.persistence.boolean.disablepopulatecache"; // Boolean
	public static final String DISABLE_AUTOSYNC_STORE = "opt.kvstore.persistence.boolean.disableautosyncstore"; // Boolean

	public static final String B_SIZE = "opt.kvstore.btree.int.bsize"; // int
	//
	final Class<K> typeK;
	final Class<V> typeV;
	//
	public KVStoreFactory(final Class<K> typeK, final Class<V> typeV) {
		this.typeK = typeK;
		this.typeV = typeV;
	}
	//
	public BplusTree<K, V> createTree(final Options opts) throws InstantiationException, IllegalAccessException {
		final String fileName = opts.getString(FILENAME);
		if (fileName == null) {
			return createTreeMemory(opts);
		} else {
			return createTreeFile(opts);
		}
	}
	public BplusTreeMemory<K, V> createTreeMemory(final Options opts) throws InstantiationException, IllegalAccessException {
		final int b_size = opts.getInt(B_SIZE, 512);
		return new BplusTreeMemory<K, V>(b_size, typeK, typeV);
	}
	public BplusTreeFile<K, V> createTreeFile(final Options opts) throws InstantiationException, IllegalAccessException {
		final String fileName = opts.getString(FILENAME);
		if (fileName == null) {
			log.error("Invalid filename for createTreeFile");
			return null;
		}
		//
		final int b_size = opts.getInt(B_SIZE, 512);
		final boolean autoTune = opts.getBoolean(AUTO_TUNE, true);
		final BplusTreeFile<K, V> tree = new BplusTreeFile<K, V>(autoTune, b_size, typeK, typeV, fileName);
		//
		final int cache_size = opts.getInt(CACHE_SIZE, 8 * 1024 * 1024);
		tree.setMaxCacheSizeInBytes(cache_size);
		final boolean useRedo = opts.getBoolean(USE_REDO, true);
		tree.setUseRedo(useRedo);
		final boolean useRedoThread = opts.getBoolean(USE_REDO_THREAD, false);
		tree.setUseRedoThread(useRedoThread);
		final boolean disablePopulateCache = opts.getBoolean(DISABLE_POPULATE_CACHE, false);
		tree.setDisablePopulateCache(disablePopulateCache);
		final boolean disableAutoSyncStore = opts.getBoolean(DISABLE_AUTOSYNC_STORE, false);
		tree.setDisableAutoSyncStore(disableAutoSyncStore);
		//
		return tree;
	}
	//
	public Options createTreeOptionsDefault() {
		final Options opts = new Options();
		opts
		.set(AUTO_TUNE, true)
		.set(B_SIZE, 512)
		.set(USE_REDO, true)
		.set(USE_REDO_THREAD, false)
		.set(CACHE_SIZE, 8*1024*1024);
		return opts;
	}
	public Options createTreeOptionsSafe() {
		final Options opts = new Options();
		opts
		.set(AUTO_TUNE, true)
		.set(B_SIZE, 512)
		.set(USE_REDO, true)
		.set(USE_REDO_THREAD, false)
		.set(CACHE_SIZE, 1*1024*1024);
		return opts;
	}
	public Options createTreeOptionsPerformance() {
		final Options opts = new Options();
		opts
		.set(AUTO_TUNE, true)
		.set(B_SIZE, 1024)
		.set(USE_REDO, false)
		.set(USE_REDO_THREAD, false)
		.set(DISABLE_AUTOSYNC_STORE, true)
		.set(CACHE_SIZE, 128*1024*1024);
		return opts;
	}
}
