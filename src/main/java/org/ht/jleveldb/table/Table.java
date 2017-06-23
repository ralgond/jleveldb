package org.ht.jleveldb.table;

import org.ht.jleveldb.Iterator0;
import org.ht.jleveldb.Options;
import org.ht.jleveldb.RandomAccessFile0;
import org.ht.jleveldb.ReadOptions;
import org.ht.jleveldb.Status;
import org.ht.jleveldb.table.Format.BlockContents;
import org.ht.jleveldb.table.Format.BlockHandle;
import org.ht.jleveldb.table.Format.Footer;
import org.ht.jleveldb.table.TwoLevelIterator.BlockFunction;
import org.ht.jleveldb.util.BytewiseComparatorImpl;
import org.ht.jleveldb.util.Cache;
import org.ht.jleveldb.util.Coding;
import org.ht.jleveldb.util.FuncOutput;
import org.ht.jleveldb.util.Slice;

public class Table {
	
	static class Rep {
		public void delete() {
			filter.delete(); //delete filter;
			filter = null;
		    filterData = null; //delete [] filter_data;
		    indexBlock.delete();
		    indexBlock = null; //delete index_block;
		}
		
		Options options;
		Status status;
		RandomAccessFile0 file;
		long cacheId;
		FilterBlockReader filter;
		byte[] filterData;

		BlockHandle metaindexHandle = new BlockHandle();  // Handle to metaindex_block: saved from footer
		Block indexBlock; //Block* index_block; 
	};
	
	Rep rep;
	
	Table(Rep rep) {
		this.rep = rep;
	}
	
	public void delete() {
		rep.delete();
	}
	

	/**
	 *  Convert an index iterator value (i.e., an encoded BlockHandle)
	 *  into an iterator over the contents of the corresponding block.
	 * @param arg
	 * @param options
	 * @param indexValue
	 * @return
	 */
	static Iterator0 blockReader(Object arg, ReadOptions options, Slice indexValue) {
		Table table = (Table)(arg);
		Cache blockCache = table.rep.options.blockCache;
		Block block = null;
		Cache.Handle cacheHandle = null;

		BlockHandle handle = new BlockHandle();
		Slice input = indexValue.clone();
		Status s = handle.decodeFrom(input);
		// We intentionally allow extra stuff in index_value so that we
		// can add more features in the future.

		if (s.ok()) {
		    BlockContents contents = new BlockContents();
		    if (blockCache != null) {
		    	byte[] cache_key_buffer = new byte[16];
		    	Coding.encodeFixedNat64(cache_key_buffer, 0, table.rep.cacheId);
		    	Coding.encodeFixedNat64(cache_key_buffer, 8, handle.offset());
		    	Slice key = new Slice(cache_key_buffer, 0, 16);
		    	cacheHandle = blockCache.lookup(key);
		    	if (cacheHandle != null) {
		    		block = (Block)(blockCache.value(cacheHandle));
		    	} else {
		    		s = Format.readBlock(table.rep.file, options, handle, contents);
		    		if (s.ok()) {
		    			block = new Block(contents);
		    			if (contents.cachable && options.fillCache) {
		    				cacheHandle = blockCache.insert(key, block, (int)block.size(), deleteCachedBlock);
		    			}
		    		}
		    	}
		    } else {
		    	s = Format.readBlock(table.rep.file, options, handle, contents);
		    	if (s.ok()) {
		    		block = new Block(contents);
		    	}
		    }
		}

		Iterator0 iter = null;
		if (block != null) {
		    iter = block.newIterator(table.rep.options.comparator);
		    if (cacheHandle == null) {
		    	iter.registerCleanup(new DeleteBlock(block));
		    } else {
		    	iter.registerCleanup(new ReleaseBlock(blockCache, cacheHandle));
		    }
		} else {
		    iter = Iterator0.newErrorIterator(s);
		}
		return iter;
	}
	
	static BlockFunction blockReaderCallback = new BlockFunction() {
		public Iterator0 run(Object arg, ReadOptions options, Slice indexValue) {
			return blockReader(arg, options, indexValue);
		}
	};
	
	static class DeleteBlock implements Runnable {
		Block block;
		
		public DeleteBlock(Block block) {
			this.block = block;
		}
		
		public void run() {
			if (block == null)
				return;
			block.delete();
			block = null;
		}
	}
	
	static class ReleaseBlock implements Runnable {
		Cache cache;
		Cache.Handle handle;
		
		public ReleaseBlock(Cache cache, Cache.Handle handle) {
			this.cache = cache;
			this.handle = handle;
		}
		
		public void run() {
			if (cache == null || handle == null)
				return;
			
			cache.release(handle);
			cache = null;
			handle = null;
		}
	}
	
	static Cache.Deleter deleteCachedBlock = new Cache.Deleter() {
		public void run(Slice key, Object value) {
			Block block = (Block)(value);
			if (block == null)
				return;
			block.delete();
			block = null;
		}
	};

	/**
	 * Returns a new iterator over the table contents.</br>
	 * The result of newIterator() is initially invalid (caller must 
	 * call one of the seek methods on the iterator before using it).
	 * @param options
	 * @return
	 */
	public Iterator0 newIterator(ReadOptions options) {
		return TwoLevelIterator.newTwoLevelIterator(
			      rep.indexBlock.newIterator(rep.options.comparator),
			      blockReaderCallback, this, options);
	}

	  // 
	/**
	 * Given a key, return an approximate byte offset in the file where 
	 * the data for that key begins (or would begin if the key were 
	 * present in the file).  The returned value is in terms of file
	 * bytes, and so includes effects like compression of the underlying data.</br>
	 * E.g., the approximate offset of the last key in the table will
	 * be close to the file length.
	 * @param key
	 * @return
	 */
	public long approximateOffsetOf(Slice key) {
		Iterator0 indexIter = rep.indexBlock.newIterator(rep.options.comparator);
		indexIter.seek(key);
		long result;
		if (indexIter.valid()) {
		    BlockHandle handle = new BlockHandle();
		    Slice input = indexIter.value();
		    Status s = handle.decodeFrom(input);
		    if (s.ok()) {
		    	result = handle.offset();
		    } else {
		      // Strange: we can't decode the block handle in the index block.
		      // We'll just return the offset of the metaindex block, which is
		      // close to the whole file size for this case.
		      result = rep.metaindexHandle.offset();
		    }
		} else {
		    // key is past the last key in the file.  Approximate the offset
		    // by returning the offset of the metaindex block (which is
		    // right near the end of the file).
		    result = rep.metaindexHandle.offset();
		}
		indexIter = null;//delete indexIter;
		return result;
	}
	  
	  
	  
	public interface HandleResult {
		void run(Object arg, Slice k, Slice v);
	}
	
	public Status internalGet(ReadOptions options, Slice k, Object arg, HandleResult handleResult) {
		 Status s = Status.ok0();
		 Iterator0 iiter = rep.indexBlock.newIterator(rep.options.comparator);
		 iiter.seek(k);
		 if (iiter.valid()) {
			 Slice handleValue = iiter.value();
			 FilterBlockReader filter = rep.filter;
		     BlockHandle handle = new BlockHandle();
		     if (filter != null &&
		        handle.decodeFrom(handleValue).ok() &&
		        !filter.keyMayMatch(handle.offset(), k)) {
		      // Not found
		      } else {
		    	  Iterator0 blockIter = blockReader(this, options, iiter.value());
		    	  blockIter.seek(k);
		    	  if (blockIter.valid()) {
		    		  handleResult.run(arg, blockIter.key(), blockIter.value());
		    	  }
		    	  s = blockIter.status();
		    	  blockIter = null; //delete block_iter;
		    }
		  }
		  if (s.ok()) {
		    s = iiter.status();
		  }
		  iiter = null; //delete iiter;
		  return s;
	}
	
	void readMeta(Footer footer) {
		if (rep.options.filterPolicy == null) {
		    return;  // Do not need any metadata
		}

		// TODO(sanjay): Skip this if footer.metaindex_handle() size indicates
		// it is an empty block.
		ReadOptions opt = new ReadOptions();
		if (rep.options.paranoidChecks) {
		    opt.verifyChecksums = true;
		}
		BlockContents contents = new BlockContents();
		if (!Format.readBlock(rep.file, opt, footer.metaindexHandle(), contents).ok()) {
		    // Do not propagate errors since meta info is not needed for operation
		    return;
		}
		Block meta = new Block(contents);

		Iterator0 iter = meta.newIterator(BytewiseComparatorImpl.getInstance());
		String key = "filter.";
		key += (rep.options.filterPolicy.name());
		iter.seek(new Slice(key));
		if (iter.valid() && iter.key().equals(new Slice(key))) {
		    readFilter(iter.value());
		}
		iter.delete(); //delete iter;
		meta.delete(); //delete meta;
	}
	
	void readFilter(Slice filterHandleValue) {
		Slice v = filterHandleValue.clone();
		BlockHandle filterHandle = new BlockHandle();
		if (!filterHandle.decodeFrom(v).ok()) {
			return;
		}

		// We might want to unify with ReadBlock() if we start
		// requiring checksum verification in Table::Open.
		ReadOptions opt = new ReadOptions();
		if (rep.options.paranoidChecks) {
		    opt.verifyChecksums = true;
		}
		BlockContents block = new BlockContents();
		if (!Format.readBlock(rep.file, opt, filterHandle, block).ok()) {
		    return;
		}
		if (block.heapAllocated) {
		    rep.filterData = block.data.data();     // Will need to delete later
		}
		rep.filter = new FilterBlockReader(rep.options.filterPolicy, block.data);
	}
		  
	
	public static Status open(Options options,
            RandomAccessFile0 file,
            long size,
            FuncOutput<Table> table) {
		table.setValue(null);
		if (size < Footer.kEncodedLength) {
			return Status.corruption("file is too short to be an sstable");
		}
		
		byte footerSpace[] = new byte[Footer.kEncodedLength];
		Slice footerInput = new Slice();
		Status s = file.read(size - Footer.kEncodedLength, Footer.kEncodedLength,
		                        footerInput, footerSpace);
		if (!s.ok()) 
			return s;
		
		Footer footer = new Footer();
		s = footer.decodeFrom(footerInput);
		if (!s.ok())
			return s;
		  
		 // Read the index block
		 BlockContents contents = new BlockContents();
		 Block indexBlock = null;
		 if (s.ok()) {
			 ReadOptions opt = new ReadOptions();
		     if (options.paranoidChecks) {
		    	 opt.verifyChecksums = true;
		     }
		     s = Format.readBlock(file, opt, footer.indexHandle(), contents);
		     if (s.ok()) {
		         indexBlock = new Block(contents);
		     }
		 }
		 
		 if (s.ok()) {
			 // We've successfully read the footer and the index block: we're
			 // ready to serve requests.
			 Rep rep = new Table.Rep();
			 rep.options = options.cloneOptions();
			 rep.file = file;
			 rep.metaindexHandle = footer.metaindexHandle();
			 rep.indexBlock = indexBlock;
			 rep.cacheId = (options.blockCache != null ? options.blockCache.newId() : 0);
			 rep.filterData = null;
			 rep.filter = null;
			 table.setValue(new Table(rep));
			 table.getValue().readMeta(footer);
		 } else {
			 indexBlock.delete();
			 indexBlock = null;
		 }
		 
		 return s;
	}
}
