package org.ht.jleveldb.table;

import org.ht.jleveldb.Iterator0;
import org.ht.jleveldb.Options;
import org.ht.jleveldb.RandomAccessFile0;
import org.ht.jleveldb.ReadOptions;
import org.ht.jleveldb.Status;
import org.ht.jleveldb.table.Format.BlockContents;
import org.ht.jleveldb.table.Format.BlockHandle;
import org.ht.jleveldb.table.Format.Footer;
import org.ht.jleveldb.util.FuncOutput;
import org.ht.jleveldb.util.Slice;

public class Table {
	
	static class Rep {
		public void delete() {
			filter.delete(); //delete filter;
			filter = null;
		    filterData = null; //delete [] filter_data;
		    //sdelete index_block;
		}
		 

		Options options;
		Status status;
		RandomAccessFile0 file;
		long cacheId;
		FilterBlockReader filter;
		byte[] filterData; //const char* filter_data

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
	
	
	  // Returns a new iterator over the table contents.
	  // The result of NewIterator() is initially invalid (caller must
	  // call one of the Seek methods on the iterator before using it).
	public Iterator0 newIterator(ReadOptions options) {
		//TODO
		return null;
	}

	  // Given a key, return an approximate byte offset in the file where
	  // the data for that key begins (or would begin if the key were
	  // present in the file).  The returned value is in terms of file
	  // bytes, and so includes effects like compression of the underlying data.
	  // E.g., the approximate offset of the last key in the table will
	  // be close to the file length.
	public long approximateOffsetOf(Slice key) {
		//TODO
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
	  
	  
	  
	interface HandleResult {
		void run(Slice k, Slice v);
	}
	
	Status internalGet(ReadOptions options, Slice k, HandleResult handleResult) {
		 Status s = Status.defaultStatus();
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
		    		  handleResult.run(blockIter.key(), blockIter.value());
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
		//TODO
	}
	
	void readFilter(Slice filterHandleValue) {
		//TODO
	}
		  
	
	public static Status open(Options options,
            RandomAccessFile0 file,
            long size,
            FuncOutput<Table> table) {
		table.setValue(null);
		if (size < Footer.EncodedLength) {
			return Status.corruption("file is too short to be an sstable");
		}
		
		byte footerSpace[] = new byte[Footer.EncodedLength];
		Slice footerInput = new Slice();
		Status s = file.read(size - Footer.EncodedLength, Footer.EncodedLength,
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
	
	
	static Iterator0 blockReader(Object arg, ReadOptions options, Slice indexValue) {
		//TODO
		return null;
	}
}
