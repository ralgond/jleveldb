package org.ht.jleveldb.table;

import org.ht.jleveldb.CompressionType;
import org.ht.jleveldb.Options;
import org.ht.jleveldb.Status;
import org.ht.jleveldb.WritableFile;
import org.ht.jleveldb.table.Format.BlockHandle;
import org.ht.jleveldb.util.ByteBuf;
import org.ht.jleveldb.util.Slice;

public class TableBuilder {
	public TableBuilder(Options options, WritableFile outFile) {
		// TODO Auto-generated constructor stub
	}

	public Status changeOptions(Options options) {
		//TODO
		return null;
	}
	
	public void add(Slice key, Slice value) {
		//TODO
	}
	
	public void flush() {
		//TODO
	}
	
	public Status finish() {
		//TODO
		return null;
	}
	
	public Status status() {
		//TODO
		return null;
	}
	
	public void abandon() {
		//TODO
	}
	
	public long numEntries() {
		//TODO
		return 0;
	}
	
	public long fileSize() {
		//TODO
		return 0;
	}
	
	boolean ok() {
		return status().ok();
	}
	
	void writeBlock(BlockBuilder block, BlockHandle handle) {
		//TODO
	}
	
	void writeRawBlock(Slice data, CompressionType compressionType, BlockHandle handle) {
		//TODO
	}
	
	static class Rep {
		Options options;
		Options indexBlockOptions;
		WritableFile file;
		long offset;
		Status status;
		BlockBuilder dataBlock;
		BlockBuilder indexBlock;
		ByteBuf lastKey;
		long numEntries;
		boolean closed; // Either Finish() or Abandon() has been called.
		FilterBlockBuilder filterBlock;
		
		public Rep(Options opt, WritableFile f) {
			options = opt.cloneOptions();
			indexBlockOptions = opt.cloneOptions();
			file = f;
			offset = 0;
			dataBlock = new BlockBuilder(options);
		}
	}
	
	
}
