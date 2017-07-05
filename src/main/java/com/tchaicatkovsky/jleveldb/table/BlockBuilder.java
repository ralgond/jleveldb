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

import com.tchaicatkovsky.jleveldb.Options;
import com.tchaicatkovsky.jleveldb.util.ByteBuf;
import com.tchaicatkovsky.jleveldb.util.ByteBufFactory;
import com.tchaicatkovsky.jleveldb.util.Slice;
import com.tchaicatkovsky.jleveldb.util.SliceFactory;

public class BlockBuilder {
	Options options;
	ByteBuf buffer; // Destination buffer
	ArrayList<Integer> restarts; // Restart points
	int counter; // Number of entries emitted since restart
	boolean finished; // Has Finish() been called?
	ByteBuf lastKey;

	public BlockBuilder(Options options) {
		assert (options.blockRestartInterval >= 1);
		this.options = options.cloneOptions();
		counter = 0;
		finished = false;
		restarts = new ArrayList<Integer>();
		restarts.add(0);
		buffer = ByteBufFactory.newUnpooled();
		lastKey = ByteBufFactory.newUnpooled();
	}

	public void reset() {
		buffer.clear();
		restarts.clear();
		restarts.add(0);
		counter = 0;
		finished = false;
		lastKey.clear();
	}
	
	public int currentSizeEstimate() {
		return (buffer.size() + 		// Raw data buffer
				restarts.size() * 4 + 	// Restart array
				4); 					// Restart array length
	}
	
	public Slice finish() {
		// Append restart array
		for (int i = 0; i < restarts.size(); i++) {
			buffer.addFixedNat32(restarts.get(i));
		}
		buffer.addFixedNat32(restarts.size());

		finished = true;
		return SliceFactory.newUnpooled(buffer);
	}

	public void add(Slice key, Slice value) {
		Slice lastKeyPiece = SliceFactory.newUnpooled(lastKey);
		assert (!finished);
		assert (counter <= options.blockRestartInterval);
		assert (buffer.empty() // No values yet?
				|| options.comparator.compare(key, lastKeyPiece) > 0);

		int shared = 0;
		if (counter < options.blockRestartInterval) {
			// See how much sharing to do with previous string
			int minLength = Integer.min(lastKeyPiece.size(), key.size());
			while ((shared < minLength) && (lastKeyPiece.getByte(shared) == key.getByte(shared))) {
				shared++;
			}
		} else {
			// Restart compression
			restarts.add(buffer.size());
			counter = 0;
		}
		int nonShared = key.size() - shared;

		// Add "<shared><non_shared><value_size>" to buffer_
		buffer.addVarNat32(shared);
		buffer.addVarNat32(nonShared);
		buffer.addVarNat32(value.size());

		// Add string delta to buffer_ followed by value
		buffer.append(key.data(), key.offset() + shared, nonShared);
		buffer.append(value.data(), value.offset(), value.size());

		lastKey.resize(shared);
		lastKey.append(key.data(), key.offset() + shared, nonShared);

		counter++;
	}



	public boolean empty() {
		return buffer.size() == 0;
	}

}
