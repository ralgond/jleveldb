package org.ht.jleveldb.table;

import org.ht.jleveldb.CompressionType;
import org.ht.jleveldb.Options;
import org.ht.jleveldb.Status;
import org.ht.jleveldb.WritableFile;
import org.ht.jleveldb.table.Format.BlockHandle;
import org.ht.jleveldb.table.Format.Footer;
import org.ht.jleveldb.util.ByteBuf;
import org.ht.jleveldb.util.ByteBufFactory;
import org.ht.jleveldb.util.Coding;
import org.ht.jleveldb.util.Crc32C;
import org.ht.jleveldb.util.Slice;
import org.ht.jleveldb.util.Snappy;

public class TableBuilder {
	
	static class Rep {
		Options options;
		Options indexBlockOptions;
		WritableFile file;
		long offset;
		Status status = Status.ok0();
		BlockBuilder dataBlock;
		BlockBuilder indexBlock;
		ByteBuf lastKey;
		long numEntries;
		boolean closed;          // Either Finish() or Abandon() has been called.
		FilterBlockBuilder filterBlock;
		
		// We do not emit the index entry for a block until we have seen the
		  // first key for the next data block.  This allows us to use shorter
		  // keys in the index block.  For example, consider a block boundary
		  // between the keys "the quick brown fox" and "the who".  We can use
		  // "the r" as the key for the index block entry since it is >= all
		  // entries in the first block and < all entries in subsequent
		  // blocks.
		  //
		  // Invariant: r->pendingIndexEntry is true only if dataBlock is empty.
		boolean pendingIndexEntry;
		BlockHandle pendingHandle = new BlockHandle();  // Handle to add to index block

		ByteBuf compressedOutput = ByteBufFactory.defaultByteBuf();
		
		public Rep(Options opt, WritableFile f) {
			options = opt.cloneOptions();
			this.file = f;
			indexBlockOptions = opt.cloneOptions();
			offset = 0;
			dataBlock = new BlockBuilder(options);
			indexBlock = new BlockBuilder(indexBlockOptions);
			numEntries = 0;
			closed = false;
			filterBlock = opt.filterPolicy == null ? null : new FilterBlockBuilder(opt.filterPolicy);
			
			pendingIndexEntry = false;
			indexBlockOptions.blockRestartInterval = 1;
		}

		public void delete() {

		}
	}
		  
	Rep rep;
	
	public TableBuilder(Options options, WritableFile file) {
		rep = new Rep(options, file);
		if (rep.filterBlock != null) {
			rep.filterBlock.startBlock(0);
		}
	}
	
	public void delete() {
		if (rep != null) {
			assert(rep.closed);  // Catch errors where caller forgot to call Finish()
			rep.filterBlock.delete();
			rep.delete();
			rep = null;
		}
	}

	public Status changeOptions(Options options) {
		// Note: if more fields are added to Options, update
		// this function to catch changes that should not be allowed to
		// change in the middle of building a Table.
		if (options.comparator != rep.options.comparator) {
		    return Status.invalidArgument("changing comparator while building table");
		}
		// Note that any live BlockBuilders point to rep_->options and therefore
		// will automatically pick up the updated options.
		rep.options = options.cloneOptions();
		rep.indexBlockOptions = options.cloneOptions();
		rep.indexBlockOptions.blockRestartInterval = 1;
		return Status.ok0();
	}
	
	public void add(Slice key, Slice value) {
		Rep r = rep;
		assert(!r.closed);
		if (!ok()) 
			return;
		if (r.numEntries > 0) {
			assert(r.options.comparator.compare(key, r.lastKey) > 0);
		}

		if (r.pendingIndexEntry) {
		    assert(r.dataBlock.empty());
		    r.options.comparator.findShortestSeparator(r.lastKey, key);
		    ByteBuf handle_encoding = ByteBufFactory.defaultByteBuf() ;
		    r.pendingHandle.encodeTo(handle_encoding);
		    r.indexBlock.add(new Slice(r.lastKey), new Slice(handle_encoding));
		    r.pendingIndexEntry = false;
		}

		if (r.filterBlock != null) {
		    r.filterBlock.addKey(key);
		}

		r.lastKey.assign(key.data(), key.size());
		r.numEntries++;
		r.dataBlock.add(key, value);

		int estimated_block_size = r.dataBlock.currentSizeEstimate();
		if (estimated_block_size >= r.options.blockSize) {
		    flush();
		}
	}
	
	public void flush() {
		Rep r = rep;
		assert(!r.closed);
		if (!ok()) 
			return;
		
		if (r.dataBlock.empty()) 
			return;
		assert(!r.pendingIndexEntry);
		writeBlock(r.dataBlock, r.pendingHandle);
		if (ok()) {
		    r.pendingIndexEntry = true;
		    r.status = r.file.flush();
		}
		if (r.filterBlock != null) {
		    r.filterBlock.startBlock(r.offset);
		}
	}
	
	public Status finish() {
		Rep r = rep;
		flush();
		assert(!r.closed);
		r.closed = true;

		BlockHandle filterBlockHandle = new BlockHandle();
		BlockHandle metaindexBlockHandle = new BlockHandle();
		BlockHandle indexBlockHandle = new BlockHandle();

		// Write filter block
		if (ok() && r.filterBlock != null) {
		    writeRawBlock(r.filterBlock.finish(), CompressionType.kNoCompression,
		                  filterBlockHandle);
		}

		// Write metaindex block
		if (ok()) {
		    BlockBuilder meta_indexBlock = new BlockBuilder(r.options);
		    if (r.filterBlock != null) {
		    	// Add mapping from "filter.Name" to location of filter data
		    	String key = "filter." + r.options.filterPolicy.name();

		    	ByteBuf handle_encoding = ByteBufFactory.defaultByteBuf();
		    	filterBlockHandle.encodeTo(handle_encoding);
		    	meta_indexBlock.add(new Slice(key), new Slice(handle_encoding));
		    }

		    // TODO(postrelease): Add stats and other meta blocks
		    writeBlock(meta_indexBlock, metaindexBlockHandle);
		}

		// Write index block
		if (ok()) {
		    if (r.pendingIndexEntry) {
		    	r.options.comparator.findShortSuccessor(r.lastKey);
		    	ByteBuf handle_encoding = ByteBufFactory.defaultByteBuf();
		    	r.pendingHandle.encodeTo(handle_encoding);
		    	r.indexBlock.add(new Slice(r.lastKey), new Slice(handle_encoding));
		    	r.pendingIndexEntry = false;
		    }
		    writeBlock(r.indexBlock, indexBlockHandle);
		}

		// Write footer
		if (ok()) {
		    Footer footer = new Footer();
		    footer.setMetaindexHandle(metaindexBlockHandle);
		    footer.setIndexHandle(indexBlockHandle);
		    ByteBuf footerEncoding = ByteBufFactory.defaultByteBuf();
		    footer.encodeTo(footerEncoding);
		    r.status = r.file.append(new Slice(footerEncoding));
		    if (r.status.ok()) {
		    	r.offset += footerEncoding.size();
		    }
		}
		return r.status;
	}
	
	public Status status() {
		return rep.status;
	}
	
	public void abandon() {
		Rep r = rep;
		assert(!r.closed);
		r.closed = true;
	}
	
	public long numEntries() {
		return rep.numEntries;
	}
	
	public long fileSize() {
		return rep.offset;
	}
	
	boolean ok() {
		return status().ok();
	}
	
	void writeBlock(BlockBuilder block, BlockHandle handle) {
		// File format contains a sequence of blocks where each block has:
		//    block_data: uint8[n]
		//    type: uint8
		//    crc: uint32
		assert(ok());
		Rep r = rep;
		Slice raw = block.finish();

		Slice block_contents = new Slice();
		CompressionType type = r.options.compression;
		// TODO(postrelease): Support more compression options: zlib?
		switch (type) {
			case kNoCompression:
				block_contents = raw;
				break;
	
			case kSnappyCompression: {
			      if (Snappy.compress(raw.data(), 0, raw.size(), r.compressedOutput) &&
			    		  r.compressedOutput.size() < raw.size() - (raw.size() / 8)) {
			    	  block_contents.init(r.compressedOutput);
			      } else {
			    	  // Snappy not supported, or compressed less than 12.5%, so just
			    	  // store uncompressed form
			    	  block_contents = raw;
			    	  type = CompressionType.kNoCompression;
			      }
			      break;
			 }
		}
		
		writeRawBlock(block_contents, type, handle);
		r.compressedOutput.clear();
		block.reset();
	}
	
	void writeRawBlock(Slice blockContents, CompressionType type, BlockHandle handle) {
		Rep r = rep;
		handle.setOffset(r.offset);
		handle.setSize(blockContents.size());
		r.status = r.file.append(blockContents);
		if (r.status.ok()) {
		    byte[] trailer = new byte[Format.kBlockTrailerSize];
		    trailer[0] = type.getType();
		    Crc32C chksum = new Crc32C();
		    chksum.update(blockContents.data(), blockContents.offset, blockContents.size());
		    chksum.update(trailer, 0, 1);
		    long crc = chksum.getValue();
		    Coding.encodeFixedNat32Long(trailer, 1, 5, Crc32C.mask(crc));
		    r.status = r.file.append(new Slice(trailer, 0, Format.kBlockTrailerSize));
		    if (r.status.ok()) {
		    	r.offset += blockContents.size() + Format.kBlockTrailerSize;
		    }
		}
	}
}
