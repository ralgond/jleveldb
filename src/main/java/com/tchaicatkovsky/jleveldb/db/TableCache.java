/**
 * Copyright (c) 2017-2018 Teng Huang <ht201509 at 163 dot com>
 * All rights reserved.
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 * This file is translated from source code file Copyright (c) 2011 
 * The LevelDB Authors and licensed under the BSD-3-Clause license.
 */

package com.tchaicatkovsky.jleveldb.db;

import com.tchaicatkovsky.jleveldb.Env;
import com.tchaicatkovsky.jleveldb.FileName;
import com.tchaicatkovsky.jleveldb.Iterator0;
import com.tchaicatkovsky.jleveldb.Options;
import com.tchaicatkovsky.jleveldb.RandomAccessFile0;
import com.tchaicatkovsky.jleveldb.ReadOptions;
import com.tchaicatkovsky.jleveldb.Status;
import com.tchaicatkovsky.jleveldb.table.Table;
import com.tchaicatkovsky.jleveldb.util.Cache;
import com.tchaicatkovsky.jleveldb.util.Coding;
import com.tchaicatkovsky.jleveldb.util.Object0;
import com.tchaicatkovsky.jleveldb.util.Slice;
import com.tchaicatkovsky.jleveldb.util.SliceFactory;

public class TableCache {

	static class TableAndFile {
		RandomAccessFile0 file;
		Table table;

		public void delete() {
			if (table != null) {
				table.delete();
				table = null;
			}
			if (file != null) {
				file.delete();
				file = null;
			}
		}
	};

	static Cache.Deleter deleteTableAndFile = new Cache.Deleter() {
		public void run(Slice key, Object value) {
			TableAndFile tf = (TableAndFile) value;
			if (tf == null)
				return;

			tf.delete();
			tf = null;
		}
	};

	static class UnrefEntry implements Runnable {
		Cache cache;
		Cache.Handle handle;

		public UnrefEntry(Cache cache, Cache.Handle handle) {
			this.cache = cache;
			this.handle = handle;
		}

		public void run() {
			if (cache == null)
				return;
			cache.release(handle);
			cache = null;
			handle = null;
		}
	}

	final static int kUint64Size = 8;
	
	Env env;
	String dbname;
	Options options;
	Cache cache;

	public TableCache(String dbname, Options options, int entries) {
		env = options.env;
		this.dbname = dbname;
		this.options = options.cloneOptions();
		cache = Cache.newLRUCache(entries);
	}

	public void delete() {
		cache.delete();
	}

	/**
	 * Return an iterator for the specified file number (the corresponding file
	 * length must be exactly "fileSize" bytes). If "table0" is non-null, also
	 * sets "table0" be the Table object underlying the returned
	 * iterator, or null if no Table object underlies the returned iterator. The
	 * returned "table0" object is owned by the cache and should not be deleted,
	 * and is valid for as long as the returned iterator is live.
	 * 
	 * @param options
	 * @param fileNumber
	 * @param fileSize
	 * @param table0
	 * @return
	 */
	public Iterator0 newIterator(ReadOptions options, long fileNumber, long fileSize, Object0<Table> table0) {
		if (table0 != null)
			table0.setValue(null);

		Object0<Cache.Handle> handle0 = new Object0<>();
		Status s = findTable(fileNumber, fileSize, handle0);
		
		if (!s.ok())
			return Iterator0.newErrorIterator(s);
		
		Cache.Handle handle = handle0.getValue();

		Table table = ((TableAndFile) cache.value(handle)).table;
				
		Iterator0 result = table.newIterator(options);
				
		result.registerCleanup(new UnrefEntry(cache, handle));
		if (table0 != null)
			table0.setValue(table);

		return result;
	}

	public Iterator0 newIterator(ReadOptions options, long fileNumber, long fileSize) {
		return newIterator(options, fileNumber, fileSize, null);
	}

	/**
	 * If a seek to internal key "k" in specified file finds an entry, call
	 * saver.run(arg, foundKey, foundValue).
	 * 
	 * @param options
	 * @param fileNumber
	 * @param fileSize
	 * @param k
	 * @param arg
	 * @param saver
	 * @return
	 */
	public Status get(ReadOptions options, long fileNumber, long fileSize, Slice k, Object arg, Table.HandleResult saver) {

		Object0<Cache.Handle> handle0 = new Object0<Cache.Handle>();
		Status s = findTable(fileNumber, fileSize, handle0);

		if (s.ok()) {
			Cache.Handle handle = handle0.getValue();
			Table t = ((TableAndFile) cache.value(handle)).table;
			s = t.internalGet(options, k, arg, saver);
			cache.release(handle);
		}
		return s;
	}


	/**
	 *  Evict any entry for the specified file number
	 * @param fileNumber
	 */
	public void evict(long fileNumber) {
		byte buf[] = new byte[kUint64Size];
		Coding.encodeFixedNat64(buf, 0, fileNumber);
		cache.erase(SliceFactory.newUnpooled(buf, 0, kUint64Size));
	}


	Status findTable(long fileNumber, long fileSize, Object0<Cache.Handle> handle) {
		Status s = Status.ok0();
		byte buf[] = new byte[kUint64Size];
		Coding.encodeFixedNat64(buf, 0, fileNumber);
		Slice key = SliceFactory.newUnpooled(buf, 0, kUint64Size);
		handle.setValue(cache.lookup(key)); // Find the TableAndFile from cache

		if (handle.getValue() == null) {
			// Insert a new TableAndFile into cache

			String fname = FileName.getTableFileName(dbname, fileNumber);
			Object0<RandomAccessFile0> file0 = new Object0<RandomAccessFile0>();

			s = env.newRandomAccessFile(fname, file0);
			if (!s.ok()) {
				String oldFname = FileName.getSSTTableFileName(dbname, fileNumber);
				if (env.newRandomAccessFile(oldFname, file0).ok()) {
					s = Status.ok0();
				}
			}

			Object0<Table> table0 = new Object0<Table>();
			if (s.ok())
				s = Table.open(options, file0.getValue(), fileSize, table0);

			if (!s.ok()) {
				assert (table0.getValue() == null);
				if (file0.getValue() != null) {
					file0.getValue().delete();
				}
				// We do not cache error results so that if the error is transient,
				// or somebody repairs the file, we recover automatically.
			} else {
				TableAndFile tf = new TableAndFile();
				tf.file = file0.getValue();
				tf.table = table0.getValue();
				handle.setValue(cache.insert(key, tf, 1, deleteTableAndFile));
			}
		}
		return s;
	}
}
