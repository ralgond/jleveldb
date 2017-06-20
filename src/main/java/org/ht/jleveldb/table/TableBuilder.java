package org.ht.jleveldb.table;

import org.ht.jleveldb.CompressionType;
import org.ht.jleveldb.Options;
import org.ht.jleveldb.Status;
import org.ht.jleveldb.WritableFile;
import org.ht.jleveldb.db.format.DBFormat;
import org.ht.jleveldb.table.Format.BlockHandle;
import org.ht.jleveldb.table.Format.Footer;
import org.ht.jleveldb.util.ByteBuf;
import org.ht.jleveldb.util.ByteBufFactory;
import org.ht.jleveldb.util.Coding;
import org.ht.jleveldb.util.Crc32c;
import org.ht.jleveldb.util.Slice;
import org.ht.jleveldb.util.Snappy;

public class TableBuilder {
	
	static class Rep {
		Options options;
		Options index_block_options;
		WritableFile file;
		long offset;
		Status status = Status.ok0();
		BlockBuilder data_block;
		BlockBuilder index_block;
		ByteBuf last_key;
		long num_entries;
		boolean closed;          // Either Finish() or Abandon() has been called.
		FilterBlockBuilder filter_block;
		
		// We do not emit the index entry for a block until we have seen the
		  // first key for the next data block.  This allows us to use shorter
		  // keys in the index block.  For example, consider a block boundary
		  // between the keys "the quick brown fox" and "the who".  We can use
		  // "the r" as the key for the index block entry since it is >= all
		  // entries in the first block and < all entries in subsequent
		  // blocks.
		  //
		  // Invariant: r->pending_index_entry is true only if data_block is empty.
		boolean pending_index_entry;
		BlockHandle pending_handle = new BlockHandle();  // Handle to add to index block

		ByteBuf compressed_output = ByteBufFactory.defaultByteBuf();
		
		public Rep(Options opt, WritableFile f) {
			options = opt.cloneOptions();
			this.file = f;
			index_block_options = opt.cloneOptions();
			offset = 0;
			data_block = new BlockBuilder(options);
			index_block = new BlockBuilder(index_block_options);
			num_entries = 0;
			closed = false;
			filter_block = opt.filterPolicy == null ? null : new FilterBlockBuilder(opt.filterPolicy);
			
			pending_index_entry = false;
			index_block_options.blockRestartInterval = 1;
		}

		public void delete() {

		}
	}
		  
	Rep rep;
	
	public TableBuilder(Options options, WritableFile file) {
		rep = new Rep(options, file);
		if (rep.filter_block != null) {
			rep.filter_block.startBlock(0);
		}
	}
	
	public void delete() {
		if (rep != null) {
			assert(rep.closed);  // Catch errors where caller forgot to call Finish()
			rep.filter_block.delete();
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
		rep.index_block_options = options.cloneOptions();
		rep.index_block_options.blockRestartInterval = 1;
		return Status.ok0();
	}
	
	public void add(Slice key, Slice value) {
		Rep r = rep;
		assert(!r.closed);
		if (!ok()) 
			return;
		if (r.num_entries > 0) {
			assert(r.options.comparator.compare(key, r.last_key) > 0);
		}

		if (r.pending_index_entry) {
		    assert(r.data_block.empty());
		    r.options.comparator.findShortestSeparator(r.last_key, key);
		    ByteBuf handle_encoding = ByteBufFactory.defaultByteBuf() ;
		    r.pending_handle.encodeTo(handle_encoding);
		    r.index_block.add(new Slice(r.last_key), new Slice(handle_encoding));
		    r.pending_index_entry = false;
		}

		if (r.filter_block != null) {
		    r.filter_block.addKey(key);
		}

		r.last_key.assign(key.data(), key.size());
		r.num_entries++;
		r.data_block.add(key, value);

		int estimated_block_size = r.data_block.currentSizeEstimate();
		if (estimated_block_size >= r.options.blockSize) {
		    flush();
		}
	}
	
	public void flush() {
		Rep r = rep;
		assert(!r.closed);
		if (!ok()) 
			return;
		
		if (r.data_block.empty()) 
			return;
		assert(!r.pending_index_entry);
		writeBlock(r.data_block, r.pending_handle);
		if (ok()) {
		    r.pending_index_entry = true;
		    r.status = r.file.flush();
		}
		if (r.filter_block != null) {
		    r.filter_block.startBlock(r.offset);
		}
	}
	
	public Status finish() {
		Rep r = rep;
		flush();
		assert(!r.closed);
		r.closed = true;

		BlockHandle filter_block_handle = new BlockHandle();
		BlockHandle metaindex_block_handle = new BlockHandle();
		BlockHandle index_block_handle = new BlockHandle();

		// Write filter block
		if (ok() && r.filter_block != null) {
		    writeRawBlock(r.filter_block.finish(), CompressionType.kNoCompression,
		                  filter_block_handle);
		}

		// Write metaindex block
		if (ok()) {
		    BlockBuilder meta_index_block = new BlockBuilder(r.options);
		    if (r.filter_block != null) {
		    	// Add mapping from "filter.Name" to location of filter data
		    	String key = "filter." + r.options.filterPolicy.name();

		    	ByteBuf handle_encoding = ByteBufFactory.defaultByteBuf();
		    	filter_block_handle.encodeTo(handle_encoding);
		    	meta_index_block.add(new Slice(key), new Slice(handle_encoding));
		    }

		    // TODO(postrelease): Add stats and other meta blocks
		    writeBlock(meta_index_block, metaindex_block_handle);
		}

		// Write index block
		if (ok()) {
		    if (r.pending_index_entry) {
		    	r.options.comparator.findShortSuccessor(r.last_key);
		    	ByteBuf handle_encoding = ByteBufFactory.defaultByteBuf();
		    	r.pending_handle.encodeTo(handle_encoding);
		    	r.index_block.add(new Slice(r.last_key), new Slice(handle_encoding));
		    	r.pending_index_entry = false;
		    }
		    writeBlock(r.index_block, index_block_handle);
		}

		// Write footer
		if (ok()) {
		    Footer footer = new Footer();
		    footer.setMetaindexHandle(metaindex_block_handle);
		    footer.setIndexHandle(index_block_handle);
		    ByteBuf footer_encoding = ByteBufFactory.defaultByteBuf();
		    footer.encodeTo(footer_encoding);
		    r.status = r.file.append(new Slice(footer_encoding));
		    if (r.status.ok()) {
		    	r.offset += footer_encoding.size();
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
		return rep.num_entries;
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
			      if (Snappy.compress(raw.data(), 0, raw.size(), r.compressed_output) &&
			    		  r.compressed_output.size() < raw.size() - (raw.size() / 8)) {
			    	  block_contents.init(r.compressed_output);
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
		r.compressed_output.clear();
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
		    long crc = Crc32c.value(blockContents.data(), blockContents.offset, blockContents.size());
		    crc = Crc32c.extend(crc, trailer, 0, 1);  // Extend crc to cover block type
		    Coding.encodeFixedNat32Long(trailer, 1, 5, Crc32c.mask(crc));
		    r.status = r.file.append(new Slice(trailer, 0, Format.kBlockTrailerSize));
		    if (r.status.ok()) {
		    	r.offset += blockContents.size() + Format.kBlockTrailerSize;
		    }
		}
	}
}
