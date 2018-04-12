/**
 * Copyright (c) 2017-2018, Teng Huang <ht201509 at 163 dot com>
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
 */

package com.tchaicatkovsky.jleveldb;

import java.util.List;

import com.tchaicatkovsky.jleveldb.util.ByteBuf;
import com.tchaicatkovsky.jleveldb.util.Object0;
import com.tchaicatkovsky.jleveldb.util.Slice;

/**
 * A DB is a persistent ordered map from keys to values.</br>
 * A DB is safe for concurrent access from multiple threads without any external synchronization.
 * 
 */
public interface DB {

	/**
	 * Open the database with the specified "name".
	 * 
	 * @param options
	 * @param name
	 * @return OK on success else a non-OK status on error.
	 * @throws Exception
	 */
	Status open(Options options, String name);

	/**
	 * Set the database entry for "key" to "value". Returns OK on success, and a non-OK status on error.</br>
	 * </br>
	 * 
	 * Note: consider setting options.sync = true.
	 * 
	 * @param options
	 * @param key
	 * @param value
	 * @return
	 * @throws Exception
	 */
	Status put(WriteOptions options, Slice key, Slice value);

	/**
	 * Remove the database entry (if any) for "key". Returns OK on success, and a non-OK status on error. It is not an error if "key" did not exist in the database.</br>
	 * </br>
	 * 
	 * Note: consider setting options.sync = true.
	 * 
	 * @param options
	 * @param key
	 * @return
	 * @throws Exception
	 */
	Status delete(WriteOptions options, Slice key);

	/**
	 * Apply the specified updates to the database. Returns OK on success, non-OK on failure.</br>
	 * </br>
	 * 
	 * Note: consider setting options.sync = true.
	 * 
	 * @param options
	 * @param updates
	 * @return
	 * @throws Exception
	 */
	Status write(WriteOptions options, WriteBatch updates);

	/**
	 * If the database contains an entry for "key" store the corresponding value in *value and return OK.</br>
	 * </br>
	 * 
	 * If there is no entry for "key" leave *value unchanged and return a status for which Status::IsNotFound() returns true.</br>
	 * </br>
	 * 
	 * May return some other Status on an error.
	 * 
	 * @param options
	 * @param key
	 * @param value
	 *            [OUTPUT]
	 * @return
	 * @throws Exception
	 */
	Status get(ReadOptions options, Slice key, ByteBuf value);

	/**
	 * Return a heap-allocated iterator over the contents of the database. The result of NewIterator() is initially invalid (caller must call one of the Seek methods on the iterator before using
	 * it).</br>
	 * </br>
	 * 
	 * Caller should delete the iterator when it is no longer needed. The returned iterator should be deleted before this db is deleted.
	 * 
	 * @param options
	 * @return
	 */
	Iterator0 newIterator(ReadOptions options);

	/**
	 * Return a handle to the current DB state. Iterators created with this handle will all observe a stable snapshot of the current DB state. The caller must call ReleaseSnapshot(result) when the
	 * snapshot is no longer needed.
	 * 
	 * @return
	 */
	Snapshot getSnapshot();

	/**
	 * Release a previously acquired snapshot. The caller must not use "snapshot" after this call.
	 * 
	 * @param snapshot
	 */
	void releaseSnapshot(Snapshot snapshot);

	/**
	 * DB implementations can export properties about their state via this method. If "property" is a valid property understood by this DB implementation, fills "*value" with its current value and
	 * returns true. Otherwise returns false.</br>
	 * </br>
	 * </br>
	 * 
	 * 
	 * Valid property names include:</br>
	 * </br>
	 *
	 * "leveldb.num-files-at-level<N>" - return the number of files at level <N>, where <N> is an ASCII representation of a level number (e.g. "0").</br>
	 * "leveldb.stats" - returns a multi-line string that describes statistics about the internal operation of the DB.</br>
	 * "leveldb.sstables" - returns a multi-line string that describes all of the sstables that make up the db contents.</br>
	 * "leveldb.approximate-memory-usage" - returns the approximate number of bytes of memory in use by the DB.</br>
	 * 
	 * @param property
	 * @param value
	 * @return
	 */
	boolean getProperty(String property, Object0<String> value);

	/**
	 * For each i in [0,n-1], store in "sizes[i]", the approximate file system space used by keys in "[range[i].start .. range[i].limit)".</br>
	 * </br>
	 * 
	 * Note that the returned sizes measure file system space usage, so if the user data compresses by a factor of ten, the returned sizes will be one-tenth the size of the corresponding user data
	 * size.</br>
	 * </br>
	 * 
	 * The results may not include the sizes of recently written data.
	 * 
	 * @param range
	 * @param n
	 * @param sizes
	 */
	void getApproximateSizes(List<Range> range, List<Long> sizes);

	/**
	 * Compact the underlying storage for the key range [begin,end]. In particular, deleted and overwritten versions are discarded, and the data is rearranged to reduce the cost of operations needed
	 * to access the data. This operation should typically only be invoked by users who understand the underlying implementation.</br>
	 * </br>
	 * 
	 * begin==null is treated as a key before all keys in the database. end==null is treated as a key after all keys in the database. Therefore the following call will compact the entire database:
	 * db.compactRange(null, null);
	 * 
	 * @param begin
	 * @param end
	 */
	void compactRange(Slice begin, Slice end) throws Exception;
	
	/**
	 * Release all resources.
	 */
	void close();

	String debugDataRange();
}
