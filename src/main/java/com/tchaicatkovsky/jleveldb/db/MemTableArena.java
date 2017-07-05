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
package com.tchaicatkovsky.jleveldb.db;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicLong;

import com.tchaicatkovsky.jleveldb.util.Slice;
import com.tchaicatkovsky.jleveldb.util.SliceFactory;

public class MemTableArena {
	
	// Allocation state
	byte[] allocPtr;
	int allocPtrOffset;
	int allocBytesRemaining;

	// Array of new[] allocated memory blocks
	ArrayList<byte[]> blocks;

	// Total memory usage of the arena.
	AtomicLong memoryUsage;
	  
	public MemTableArena() {
		blocks = new ArrayList<byte[]>();
		memoryUsage = new AtomicLong(0);
	}
	
	public void delete() {
		blocks.clear();
	}
	
	/**
	 * Return a pointer to a newly allocated memory block of "bytes" bytes.
	 * 
	 * @param bytes
	 * @return
	 */
	public Slice allocate(int bytes) {
		// The semantics of what to return are a bit messy if we allow
		// 0-byte allocations, so we disallow them here (we don't need
		// them for our internal use).
		assert(bytes > 0);
		
		if (bytes <= allocBytesRemaining) {
		    byte[] result = allocPtr;
		    int oldAllocPtrOffset = allocPtrOffset;
		    allocPtrOffset += bytes;
		    allocBytesRemaining -= bytes;
		    return SliceFactory.newUnpooled(result, oldAllocPtrOffset, bytes);
		}
		
		return allocateFallback(bytes);
	}
	
	
	public long memoryUsage() {
		return memoryUsage.get();
	}
	
	static final int kBlockSize = 4096;
	
	Slice allocateFallback(int bytes) {
		if (bytes > kBlockSize / 4) {
		    // Object is more than a quarter of our block size.  Allocate it separately
		    // to avoid wasting too much space in leftover bytes.
		    byte[] result = allocateNewBlock(bytes);
		    return SliceFactory.newUnpooled(result, 0, bytes);
		}

		// We waste the remaining space in the current block.
		allocPtr = allocateNewBlock(kBlockSize);
		allocPtrOffset = 0;
		allocBytesRemaining = kBlockSize;

		int oldAllocPtrOffset = allocPtrOffset;
		allocPtrOffset += bytes;
		allocBytesRemaining -= bytes;
		
		return SliceFactory.newUnpooled(allocPtr, oldAllocPtrOffset, allocBytesRemaining);
	}
	
	byte[] allocateNewBlock(int blockBytes) {
		byte[] result = new byte[blockBytes];
		blocks.add(result);
		memoryUsage.set(memoryUsage() + blockBytes + 8); //TODO: 32bit: 4, 64bit: 8
		return result;
	}
}
