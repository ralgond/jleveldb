package org.ht.jleveldb.table;

import org.ht.jleveldb.Iterator0;
import org.ht.jleveldb.Status;
import org.ht.jleveldb.table.Format.BlockContents;
import org.ht.jleveldb.util.ByteBuf;
import org.ht.jleveldb.util.ByteBufFactory;
import org.ht.jleveldb.util.Coding;
import org.ht.jleveldb.util.Comparator0;
import org.ht.jleveldb.util.Integer0;
import org.ht.jleveldb.util.Slice;

public class Block {
	Slice data;
	int size;
	int restartOffset;
	boolean owned;
	
	public Block(BlockContents contents) {
		data = contents.data;
		size = data.size();
		owned = contents.heapAllocated;
		if (data.size() < 4) { //size_ < sizeof(uint32_t)
			size = 0; //Error marker
		} else {
			int maxRestartsAllowed = (size - 4) / 4; //size_t max_restarts_allowed = (size_-sizeof(uint32_t)) / sizeof(uint32_t);
			if (numRestarts() > maxRestartsAllowed) {
				// The size is too small for NumRestarts()
				size = 0;
			} else {
			    restartOffset = data.offset + size - (1 + numRestarts()) * 4; //restart_offset_ = size_ - (1 + NumRestarts()) * sizeof(uint32_t);
		    }
		}
	}
	
	public void delete() {
		data = null;
	}
	
	public long size() { 
		return size; 
	}
	
	public Iterator0 newIterator(Comparator0 comparator) {
		if (size < 4) { //if (size_ < sizeof(uint32_t)) {
		    return Iterator0.newErrorIterator(Status.corruption("bad block contents"));
		}
		
		int numRestarts = numRestarts();
		
		if (numRestarts == 0) {
			return Iterator0.newEmptyIterator();
		} else {
		    return new Iter(comparator, data.data, data.offset, restartOffset, numRestarts);
		}
	}
	
	int numRestarts() {
		assert(size >= 4); //assert(size_ >= sizeof(uint32_t));
		return Coding.decodeFixedNat32(data.data, data.offset + size - 4); //DecodeFixed32(data_ + size_ - sizeof(uint32_t));
	}
	
	static class Iter extends Iterator0 {
		
		Comparator0 comparator;
		byte data[]; 			// Underlying block contents
		int dataOffset;
		int restartsOffset;     // Offset of restart array (list of fixed32)
		int numRestarts; 		// Number of uint32 entries in restart array

		// current is offset in data of current entry.  >= restartsOffset if !valid()
		int current;
		int restartIndex;  // Index of restart block in which current falls
		ByteBuf key;
		Slice value;
		Status status;
		
		public Iter(Comparator0 comparator,
			       byte[] data,
			       int dataOffset,
			       int restartsOffset,
			       int numRestarts) {
			this.comparator = comparator;
			this.data = data;
			this.dataOffset = dataOffset;
			this.restartsOffset = restartsOffset;
			this.numRestarts = numRestarts;
			
			this.current = restartsOffset;
			this.restartIndex = numRestarts;
			key = ByteBufFactory.defaultByteBuf();
			value = new Slice(data, dataOffset, 0);
			assert(this.numRestarts > 0);
			this.status = Status.ok0();
		}
		
		
		public void delete() {
			super.delete();
			comparator = null;
			data = null;
			key = null;
			value = null;
		}
		
		final int compare(Slice a, Slice b) {
		    return comparator.compare(a, b);
		}
		
		final int compare(ByteBuf a, Slice b) {
		    return comparator.compare(a, b);
		}
		
		// Return the offset in data_ just past the end of the current entry.
		final int nextEntryOffset() {
			//return value.offset + value.size(); //return (value_.data() + value_.size()) - data_;
			return value.offset + value.size();
		}
		
		final int getRestartPoint(int index) {
		    assert(index < numRestarts);
		    //DecodeFixed32(data_ + restarts_ + index * sizeof(uint32_t));
		    return Coding.decodeFixedNat32(data, restartsOffset + index * 4) + dataOffset; 		
		}
		
		void seekToRestartPoint(int index) {
		    key.clear();
		    restartIndex = index;
		    // current will be fixed by parseNextKey();

		    // parseNextKey() starts at the end of value, so set value accordingly
		    int offset = getRestartPoint(index);
		    value = new Slice(data, offset, 0);	//value_ = Slice(data_ + offset, 0);
		}
		
		public boolean valid() {
			return current < restartsOffset; 
		}
		
		public void seekToFirst() {
			seekToRestartPoint(0);
		    parseNextKey();
		}
		
		public void seekToLast() {
			 seekToRestartPoint(numRestarts - 1);
			 while (parseNextKey() && nextEntryOffset() < restartsOffset) {
			      // Keep skipping
			 }
		}
		
		public void seek(Slice target) {
			// Binary search in restart array to find the last restart point
		    // with a key < target
			
		    int left = 0;
		    int right = numRestarts - 1;
		    Integer0 shared = new Integer0();
		    Integer0 nonShared = new Integer0();
		    Integer0 valueLength = new Integer0();
		    Slice slice = new Slice();
		    Slice midKey = new Slice();
		    while (left < right) {
		    	int mid = (left + right + 1) / 2;
		    	int regionOffset = getRestartPoint(mid);
		    	
		    	shared.setValue(0);
		    	nonShared.setValue(0);
		    	valueLength.setValue(0);
		    	slice.init(data, regionOffset, restartsOffset - regionOffset);
		    	boolean ret = decodeEntry(slice, shared, nonShared, valueLength);
		    	if (!ret || shared.getValue() != 0) {
		    		corruptionError();
		    		return;
		    	}
		    	
		    	//Slice mid_key(key_ptr, non_shared);
		    	midKey.init(slice.data, slice.offset, nonShared.getValue());
		    	if (compare(midKey, target) < 0) {
		            // Key at "mid" is smaller than "target".  Therefore all
		            // blocks before "mid" are uninteresting.
		            left = mid;
		        } else {
		            // Key at "mid" is >= "target".  Therefore all blocks at or
		            // after "mid" are uninteresting.
		            right = mid - 1;
		        }
		    }
		    
		    // Linear search (within restart block) for first key >= target
		    seekToRestartPoint(left);
		    while (true) {
		        if (!parseNextKey()) {
		        	return;
		        }
		        int cmpRet = compare(key, target);
		        if (cmpRet >= 0) {
		        	return;
		        }
		    }
		}
		
		public void next() {
			assert(valid());
		    parseNextKey();
		}
		
		public void prev() {
			assert(valid());

		    // Scan backwards to a restart point before current_
		    int original = current;
		    while (getRestartPoint(restartIndex) >= original) {
		      if (restartIndex == 0) {
		        // No more entries
		        current = restartsOffset;
		        restartIndex = numRestarts;
		        return;
		      }
		      restartIndex--;
		    }

		    seekToRestartPoint(restartIndex);
		    do {
		      // Loop until end of current entry hits the start of original entry
		    } while (parseNextKey() && nextEntryOffset() < original);
		}
		
		public Slice key() {
			assert(valid());
		    return new Slice(key);
		}
		
		public Slice value() {
			assert(valid());
		    return value;
		}
		
		public Status status() {
			return status;
		}
		
		void corruptionError() {
		    current = restartsOffset;
		    restartIndex = numRestarts;
		    status = Status.corruption("bad entry in block");
		    key.clear();
		    value.clear();
		}
		
		boolean parseNextKey() {
		    current = nextEntryOffset();
		    
		    if (current >= restartsOffset) {
		    	// No more entries to return.  Mark as invalid.
		    	current = restartsOffset;
		    	restartIndex = numRestarts;
		    	return false;
		    }
		    
		    Slice slice = new Slice(data, current, restartsOffset - current);
		    
		    // Decode next entry
		    Integer0 shared = new Integer0();
		    Integer0 nonShared = new Integer0();
		    Integer0 valueLength = new Integer0();
		    
		    boolean ret = decodeEntry(slice, shared, nonShared, valueLength);
		    
		    if (!ret || key.size() < shared.getValue()) {
		    	corruptionError();
		    	return false;
		    } else {
		    	key.resize(shared.getValue());
		    	key.append(slice.data, slice.offset, nonShared.getValue());
		    	value = new Slice(slice.data, slice.offset+nonShared.getValue(), valueLength.getValue());
		    	
		    	while (restartIndex + 1 < numRestarts &&
		                getRestartPoint(restartIndex + 1) < current) {
		           ++restartIndex;
		    	}
		    	return true;
		    }
		}
	}
	
	public static boolean decodeEntry(Slice slice, 
			Integer0 shared, 
			Integer0 nonShared,
			Integer0 valueLength) {
		
		if (slice.size() < 3) 
			return false;
		
		shared.setValue(slice.data[slice.offset] & 0x0ff);
		nonShared.setValue(slice.data[slice.offset+1] & 0x0ff);
		valueLength.setValue(slice.data[slice.offset+2] & 0x0ff);
		
		 if ((shared.getValue() | nonShared.getValue() | valueLength.getValue()) < 128) {
			 // Fast path: all three values are encoded in one byte each
			 slice.offset += 3;
		 } else {
			 try {
				 shared.setValue(Coding.getVarNat32Ptr(slice));
				 nonShared.setValue(Coding.getVarNat32Ptr(slice));
				 valueLength.setValue(Coding.getVarNat32Ptr(slice));
			 } catch (Exception e) {
				 return false;
			 }
		 }
		 
		 if (slice.size() < nonShared.getValue() - valueLength.getValue()) {
			 return false;
		 }
		 
		 return true;
	}
}
