/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tchaicatkovsky.jleveldb;

import java.util.List;

import com.tchaicatkovsky.jleveldb.util.BloomFilterPolicy;
import com.tchaicatkovsky.jleveldb.util.ByteBuf;
import com.tchaicatkovsky.jleveldb.util.Slice;

public abstract class FilterPolicy {
	/**
	 * Return the name of this policy. Note that if the filter encoding changes in an incompatible way, the name returned by this method must be changed. Otherwise, old incompatible filters may be
	 * passed to methods of this type.
	 * 
	 * @return
	 */
	public abstract String name();

	/**
	 * keys[0,n-1] contains a list of keys (potentially with duplicates) that are ordered according to the user supplied comparator. Append a filter that summarizes keys[0,n-1] to dst.</br>
	 * </br>
	 * 
	 * Warning: do not change the initial contents of dst. Instead, append the newly constructed filter to *dst.
	 * 
	 * @param keys
	 * @param buf
	 */
	public abstract void createFilter(List<Slice> keys, ByteBuf buf);

	/**
	 * "filter" contains the data appended by a preceding call to createFilter() on this class. This method must return true if the key was in the list of keys passed to createFilter().</br>
	 * This method may return true or false if the key was not on the list, but it should aim to return false with a high probability.
	 * 
	 * @param key
	 * @param filter
	 * @return
	 */
	public abstract boolean keyMayMatch(Slice key, Slice filter);

	/**
	 * Return a new filter policy that uses a bloom filter with approximately the specified number of bits per key. A good value for bits_per_key is 10, which yields a filter with ~ 1% false positive
	 * rate.</br>
	 * </br>
	 * 
	 * Callers must delete the result after any database that is using the result has been closed.</br>
	 * </br>
	 * 
	 * Note: if you are using a custom comparator that ignores some parts of the keys being compared, you must not use newBloomFilterPolicy() and must provide your own FilterPolicy that also ignores
	 * the corresponding parts of the keys. For example, if the comparator ignores trailing spaces, it would be incorrect to use a FilterPolicy (like newBloomFilterPolicy) that does not ignore
	 * trailing spaces in keys.
	 * 
	 * @param bitsPerKey
	 * @return
	 */
	public static FilterPolicy newBloomFilterPolicy(int bitsPerKey) {
		return new BloomFilterPolicy(bitsPerKey);
	}

	public abstract void delete();
}
