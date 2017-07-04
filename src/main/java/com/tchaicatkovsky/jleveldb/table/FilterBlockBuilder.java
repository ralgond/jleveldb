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
package com.tchaicatkovsky.jleveldb.table;

import java.util.ArrayList;

import com.tchaicatkovsky.jleveldb.FilterPolicy;
import com.tchaicatkovsky.jleveldb.util.ByteBuf;
import com.tchaicatkovsky.jleveldb.util.ByteBufFactory;
import com.tchaicatkovsky.jleveldb.util.DefaultSlice;
import com.tchaicatkovsky.jleveldb.util.Slice;

public class FilterBlockBuilder {
	final public static int kFilterBaseLg = 11;
	final public static int kFilterBase = 1 << kFilterBaseLg;

	FilterPolicy policy;
	
	ByteBuf keys = ByteBufFactory.defaultByteBuf(); // Flattened key contents
	
	ArrayList<Integer> start = new ArrayList<>(); // Starting index in keys_of each key
	
	ByteBuf result = ByteBufFactory.defaultByteBuf(); // Filter data computed so far
	
	ArrayList<Slice> tmpKeys = new ArrayList<>(); // policy.createFilter() argument
	
	ArrayList<Integer> filterOffsets = new ArrayList<>();

	
	public FilterBlockBuilder(FilterPolicy policy) {
		this.policy = policy;
	}

	public void delete() {

	}

	public void startBlock(long blockOffset) {
		long filterIndex = (blockOffset / kFilterBase);
		assert (filterIndex >= filterOffsets.size());
		while (filterIndex > filterOffsets.size()) {
			generateFilter();
		}
	}

	public void addKey(Slice key) {
		start.add(keys.size());
		keys.append(key.data(), key.offset(), key.size());
	}

	public Slice finish() {
		if (!start.isEmpty()) {
			generateFilter();
		}

		// Append array of per-filter offsets
		int arrayOffset = result.size();
		for (int i = 0; i < filterOffsets.size(); i++)
			result.writeFixedNat32(filterOffsets.get(i));

		result.writeFixedNat32(arrayOffset);

		result.addByte((byte) (kFilterBaseLg & 0xff)); // Save encoding parameter in result

		return new DefaultSlice(result);
	}

	public void generateFilter() {
		int numKeys = start.size();
		if (numKeys == 0) {
			// Fast path if there are no keys for this filter
			filterOffsets.add(result.size());
			return;
		}

		// Make list of keys from flattened key structure
		start.add(keys.size()); // Simplify length computation
		tmpKeys.clear();
		for (int i = 0; i < numKeys; i++) {
			byte[] base = keys.data();
			int baseOffset = start.get(i);
			int length = start.get(i + 1) - start.get(i); // bytes
			tmpKeys.add(new DefaultSlice(base, baseOffset, length));
		}

		// Generate filter for current set of keys and append to result.
		filterOffsets.add(result.size());
		policy.createFilter(tmpKeys, result);

		tmpKeys.clear();
		keys.clear();
		start.clear();
	}
}
