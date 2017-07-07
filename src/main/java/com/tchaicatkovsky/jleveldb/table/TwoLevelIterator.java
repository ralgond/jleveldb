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

import com.tchaicatkovsky.jleveldb.Iterator0;
import com.tchaicatkovsky.jleveldb.ReadOptions;
import com.tchaicatkovsky.jleveldb.Status;
import com.tchaicatkovsky.jleveldb.util.ByteBuf;
import com.tchaicatkovsky.jleveldb.util.ByteBufFactory;
import com.tchaicatkovsky.jleveldb.util.Slice;

public class TwoLevelIterator extends Iterator0 {
	
	public interface BlockFunction {
		Iterator0 run(Object arg, ReadOptions options, Slice indexValue);
	}
	
	/**
	 * Return a new two level iterator.  A two-level iterator contains an
	 * index iterator whose values point to a sequence of blocks where
	 * each block is itself a sequence of key,value pairs.  The returned
	 * two-level iterator yields the concatenation of all key/value pairs
	 * in the sequence of blocks.  Takes ownership of "indexIter" and 
	 * will delete it when no longer needed.</br></br>
	 * 
	 * Uses a supplied function to convert an indexIter value into 
	 * an iterator over the contents of the corresponding block.
	 * 
	 * @param indexIter
	 * @param blockFunction
	 * @param arg
	 * @param options
	 * @return
	 */
	public static Iterator0 newTwoLevelIterator(Iterator0 indexIter, BlockFunction blockFunction, Object arg, ReadOptions options) {
		return new TwoLevelIterator(indexIter, blockFunction, arg, options);
	}
	
	BlockFunction blockFunction;
	Object arg;
	ReadOptions options;
	Status status = Status.ok0();
	Iterator0Wrapper indexIter = new Iterator0Wrapper();
	Iterator0Wrapper dataIter = new Iterator0Wrapper(); // May be null
	/**
	 * If dataIter is non-null, then "dataBlockHandle" holds the
	 * "indexValue" passed to blockFunction to create the dataIter.
	 */
	ByteBuf dataBlockHandle = ByteBufFactory.newUnpooled();
	
	public TwoLevelIterator(Iterator0 indexIter0, BlockFunction blockFunction, Object arg, ReadOptions options) {
		this.indexIter.set(indexIter0);
		this.blockFunction = blockFunction;
		this.arg = arg;
		this.options = options.clone();
	}
	
	public void delete() {
		super.delete();
		if (indexIter != null) {
			indexIter.delete();
			indexIter = null;
		}
		
		if (dataIter != null) {
			dataIter.delete();
			dataIter = null;
		}
		
		if (blockFunction != null) {
			blockFunction = null;
		}
		if (arg != null) {
			arg = null;
		}
		if (options != null) {
			options = null;
		}
	}

	public boolean valid() {
		return dataIter.valid();
	}
	
	public void seekToFirst() {
		indexIter.seekToFirst();
		initDataBlock();
		if (dataIter.iter() != null) {
			dataIter.seekToFirst();
		}
		skipEmptyDataBlocksForward();
	}
	
	public void seekToLast() {
		indexIter.seekToLast();
		initDataBlock();
		if (dataIter.iter() != null) 
			dataIter.seekToLast();
		skipEmptyDataBlocksBackward();
	}
	
	public void seek(Slice target) {
		indexIter.seek(target);
		initDataBlock();
		if (dataIter.iter() != null) 
			dataIter.seek(target);
		skipEmptyDataBlocksForward();
	}
	
	public void next() {
		assert(valid());
		dataIter.next();
		skipEmptyDataBlocksForward();		
	}
	
	public void prev() {
		assert(valid());
		dataIter.prev();
		skipEmptyDataBlocksBackward();
	}
	
	public Slice key() {
		assert(valid());
		return dataIter.key();
	}
	
	public Slice value() {
		assert(valid());
		return dataIter.value();
	}
	
	public Status status() {
	    if (!indexIter.status().ok()) {
	    	return indexIter.status();
	    } else if (dataIter.iter() != null && !dataIter.status().ok()) {
	    	return dataIter.status();
	    } else {
	    	return status;
	    }
	}

	void saveError(Status s) {
	    if (status.ok() && !s.ok()) 
	    	status = s;
	}
	
	void skipEmptyDataBlocksForward(){
		while (dataIter.iter() == null || !dataIter.valid()) {
			// Move to next block
			if (!indexIter.valid()) {
				setDataIterator(null);
			    return;
			}
			indexIter.next();
			initDataBlock();
			if (dataIter.iter() != null) 
				dataIter.seekToFirst();
		}
	}
	
	void skipEmptyDataBlocksBackward() {
		while (dataIter.iter() == null || !dataIter.valid()) {
			// Move to next block
			if (!indexIter.valid()) {
				setDataIterator(null);
				return;
			}
			indexIter.prev();
			initDataBlock();
			if (dataIter.iter() != null) 
				dataIter.seekToLast();
		}
	}
	
	void setDataIterator(Iterator0 dataIter0) {
		if (dataIter.iter() != null) 
			saveError(dataIter.status());
		dataIter.set(dataIter0);
	}
	
	void initDataBlock() {
		if (!indexIter.valid()) {
			setDataIterator(null);
		} else {
			Slice handle = indexIter.value();
			if (dataIter.iter() != null && handle.compare(dataBlockHandle) == 0) {
				// dataIter is already constructed with this iterator, so
			    // no need to change anything
			} else {
				Iterator0 iter = blockFunction.run(arg, options, handle);
				dataBlockHandle.assign(handle.data(), handle.offset(), handle.size());
			    setDataIterator(iter);
			}
		}
	}
}
