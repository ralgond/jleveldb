package com.tchaicatkovsky.jleveldb.util;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicLong;

public class Arena {
	
	// Allocation state
	byte[] allocPtr;
	int allocPtrOffset;
	int allocBytesRemaining;

	// Array of new[] allocated memory blocks
	ArrayList<byte[]> blocks;

	// Total memory usage of the arena.
	AtomicLong memoryUsage;
	  
	public Arena() {
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
		    return new DefaultSlice(result, oldAllocPtrOffset, bytes);
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
		    return new DefaultSlice(result, 0, bytes);
		}

		// We waste the remaining space in the current block.
		allocPtr = allocateNewBlock(kBlockSize);
		allocPtrOffset = 0;
		allocBytesRemaining = kBlockSize;

		int oldAllocPtrOffset = allocPtrOffset;
		allocPtrOffset += bytes;
		allocBytesRemaining -= bytes;
		
		return new DefaultSlice(allocPtr, oldAllocPtrOffset, allocBytesRemaining);
	}
	
	byte[] allocateNewBlock(int blockBytes) {
		byte[] result = new byte[blockBytes];
		blocks.add(result);
		memoryUsage.set(memoryUsage() + blockBytes + 8); //TODO: 32bit: 4, 64bit: 8
		return result;
	}
}
