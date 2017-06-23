package org.ht.jleveldb.table;

import org.ht.jleveldb.Iterator0;
import org.ht.jleveldb.ReadOptions;
import org.ht.jleveldb.Status;
import org.ht.jleveldb.util.ByteBuf;
import org.ht.jleveldb.util.Slice;

public class TwoLevelIterator extends Iterator0 {
	
	public interface BlockFunction {
		Iterator0 run(Object arg, ReadOptions options, Slice indexValue);
	}
	
	// Return a new two level iterator.  A two-level iterator contains an
	// index iterator whose values point to a sequence of blocks where
	// each block is itself a sequence of key,value pairs.  The returned
	// two-level iterator yields the concatenation of all key/value pairs
	// in the sequence of blocks.  Takes ownership of "index_iter" and
	// will delete it when no longer needed.
	//
	// Uses a supplied function to convert an index_iter value into
	// an iterator over the contents of the corresponding block.
	public static Iterator0 newTwoLevelIterator(Iterator0 indexIter, BlockFunction blockFunction, Object arg, ReadOptions options) {
		return new TwoLevelIterator(indexIter, blockFunction, arg, options);
	}
	
	public TwoLevelIterator(Iterator0 indexIter0, BlockFunction blockFunction, Object arg, ReadOptions options) {
		this.indexIter = new Iterator0Wrapper();
		this.indexIter.set(indexIter0);
		this.blockFunction = blockFunction;
		this.arg = arg;
		this.options = options.clone();
	}
	
	public void delete() {
		if (indexIter != null) {
			indexIter.delete();
			indexIter = null;
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
		if (dataIter.iter() != null) dataIter.seekToFirst();
		skipEmptyDataBlocksForward();
	}
	
	public void seekToLast() {
		indexIter.seekToLast();
		initDataBlock();
		if (dataIter.iter() != null) dataIter.seekToLast();
		skipEmptyDataBlocksBackward();
	}
	
	public void seek(Slice target) {
		indexIter.seek(target);
		initDataBlock();
		if (dataIter.iter() != null) dataIter.seek(target);
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
		// It'd be nice if status() returned a const Status& instead of a Status
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
			if (dataIter.iter() != null) dataIter.seekToFirst();
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
			if (dataIter.iter() != null) dataIter.seekToLast();
		}
	}
	
	void setDataIterator(Iterator0 dataIter0) {
		if (dataIter.iter() != null) saveError(dataIter.status());
		dataIter.set(dataIter0);
	}
	
	void initDataBlock() {
		if (!indexIter.valid()) {
			setDataIterator(null);
		} else {
			Slice handle = indexIter.value();
			if (dataIter.iter() != null && handle.compare(new Slice(dataBlockHandle)) == 0) {
				// data_iter_ is already constructed with this iterator, so
			    // no need to change anything
			} else {
				Iterator0 iter = blockFunction.run(arg, options, handle);
				dataBlockHandle.assign(handle.data(), handle.size());
			    setDataIterator(iter);
			}
		}
	}
	  
	BlockFunction blockFunction;
	Object arg;
	ReadOptions options;
	Status status;
	Iterator0Wrapper indexIter;
	Iterator0Wrapper dataIter; // May be NULL
	// If data_iter_ is non-NULL, then "data_block_handle_" holds the
	// "index_value" passed to block_function_ to create the data_iter_.
	ByteBuf dataBlockHandle;
}
