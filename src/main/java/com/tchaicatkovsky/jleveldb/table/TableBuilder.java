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

import com.tchaicatkovsky.jleveldb.CompressionType;
import com.tchaicatkovsky.jleveldb.Options;
import com.tchaicatkovsky.jleveldb.Status;
import com.tchaicatkovsky.jleveldb.WritableFile;
import com.tchaicatkovsky.jleveldb.table.TableFormat.BlockHandle;
import com.tchaicatkovsky.jleveldb.table.TableFormat.Footer;
import com.tchaicatkovsky.jleveldb.util.ByteBuf;
import com.tchaicatkovsky.jleveldb.util.ByteBufFactory;
import com.tchaicatkovsky.jleveldb.util.Coding;
import com.tchaicatkovsky.jleveldb.util.Crc32C;
import com.tchaicatkovsky.jleveldb.util.Slice;
import com.tchaicatkovsky.jleveldb.util.SliceFactory;
import com.tchaicatkovsky.jleveldb.util.Snappy;

public class TableBuilder {

	static class Rep {
		Options options;
		Options indexBlockBuilderOptions;
		WritableFile file;
		long offset;
		Status status = Status.ok0();
		BlockBuilder dataBlockBuilder;
		BlockBuilder indexBlockBuilder;
		FilterBlockBuilder filterBlockBuilder;
		ByteBuf lastKey;
		long numEntries;
		boolean closed; // Either finish() or abandon() has been called.

		/**
		 * We do not emit the index entry for a block until we have seen the first key
		 * for the next data block. This allows us to use shorter keys in the index
		 * block. For example, consider a block boundary between the keys "the quick
		 * brown fox" and "the who". We can use "the r" as the key for the index block
		 * entry since it is >= all entries in the first block and < all entries in
		 * subsequent blocks.</br></br>
		 * 
		 * Invariant: pendingIndexEntry is true only if dataBlockBuilder is empty.
		 */
		boolean pendingIndexEntry;
		BlockHandle pendingHandle = new BlockHandle(); // Handle to add to index block

		ByteBuf compressedOutput = ByteBufFactory.newUnpooled();

		public Rep(Options opt, WritableFile f) {
			options = opt.cloneOptions();
			this.file = f;
			indexBlockBuilderOptions = opt.cloneOptions();
			offset = 0;
			dataBlockBuilder = new BlockBuilder(options);
			indexBlockBuilder = new BlockBuilder(indexBlockBuilderOptions);
			lastKey = ByteBufFactory.newUnpooled();
			numEntries = 0;
			closed = false;
			filterBlockBuilder = opt.filterPolicy == null ? null : new FilterBlockBuilder(opt.filterPolicy);

			pendingIndexEntry = false;
			indexBlockBuilderOptions.blockRestartInterval = 1;
		}

		public void delete() {
			
		}
	}

	Rep rep;

	public TableBuilder(Options options, WritableFile file) {
		rep = new Rep(options, file);
		if (rep.filterBlockBuilder != null) {
			rep.filterBlockBuilder.startBlock(0);
		}
	}

	public void delete() {
		if (rep != null) {
			assert (rep.closed); // Catch errors where caller forgot to call finish()
			if (rep.filterBlockBuilder != null)
				rep.filterBlockBuilder.delete();
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
		rep.indexBlockBuilderOptions = options.cloneOptions();
		rep.indexBlockBuilderOptions.blockRestartInterval = 1;
		return Status.ok0();
	}

	public void add(Slice key, Slice value) {
		Rep r = rep;
		assert (!r.closed);
		if (!ok())
			return;

		if (r.numEntries > 0) {
			int ret = r.options.comparator.compare(key, r.lastKey);			
			assert (ret > 0);
		}
		
		if (r.pendingIndexEntry) {
			assert (r.dataBlockBuilder.empty());
			r.options.comparator.findShortestSeparator(r.lastKey, key);
			ByteBuf handleEncoding = ByteBufFactory.newUnpooled();
			r.pendingHandle.encodeTo(handleEncoding);
			r.indexBlockBuilder.add(SliceFactory.newUnpooled(r.lastKey), SliceFactory.newUnpooled(handleEncoding)); // TODO: new DefaultSlice(r.lastKey)->r.lastKey
			r.pendingIndexEntry = false;
		}
		

		if (r.filterBlockBuilder != null) {
			r.filterBlockBuilder.addKey(key);
		}
		

		r.lastKey.assign(key.data(), key.offset(), key.size());
		r.numEntries++;
		r.dataBlockBuilder.add(key, value);

		int estimatedBlockSize = r.dataBlockBuilder.currentSizeEstimate();
		if (estimatedBlockSize >= r.options.blockSize) {
			flush();
		}
	}

	/**
	 * write current block to file and start accepting key value data 
	 * for next block.
	 */
	void flush() {
		
		Rep r = rep;
		assert (!r.closed);
		if (!ok())
			return;

		if (r.dataBlockBuilder.empty())
			return;

		assert (!r.pendingIndexEntry);
		writeBlock(r.dataBlockBuilder, r.pendingHandle);
		if (ok()) {
			r.pendingIndexEntry = true;
			r.status = r.file.flush();
		}
		if (r.filterBlockBuilder != null) {
			r.filterBlockBuilder.startBlock(r.offset);
		}
	}

	public Status finish() {
		Rep r = rep;
		flush();
		assert (!r.closed);
		r.closed = true;

		BlockHandle filterBlockHandle = new BlockHandle();
		BlockHandle metaindexBlockHandle = new BlockHandle();
		BlockHandle indexBlockHandle = new BlockHandle();

		// Write filter block
		if (ok() && r.filterBlockBuilder != null) {
			writeRawBlock(r.filterBlockBuilder.finish(), CompressionType.kNoCompression, filterBlockHandle);
		}

		// Write metaindex block
		if (ok()) {
			BlockBuilder metaIndexBlockBuilder = new BlockBuilder(r.options);
			if (r.filterBlockBuilder != null) {
				// Add mapping from "filter.Name" to location of filter data
				String key = "filter." + r.options.filterPolicy.name();

				ByteBuf handleEncoding = ByteBufFactory.newUnpooled();
				filterBlockHandle.encodeTo(handleEncoding);
				metaIndexBlockBuilder.add(SliceFactory.newUnpooled(key), SliceFactory.newUnpooled(handleEncoding));
			}

			// TODO(postrelease): Add stats and other meta blocks
			writeBlock(metaIndexBlockBuilder, metaindexBlockHandle);
		}

		// Write index block
		if (ok()) {
			if (r.pendingIndexEntry) {
				assert (r.dataBlockBuilder.empty());
				r.options.comparator.findShortSuccessor(r.lastKey);
				ByteBuf handleEncoding = ByteBufFactory.newUnpooled();
				r.pendingHandle.encodeTo(handleEncoding);
				r.indexBlockBuilder.add(SliceFactory.newUnpooled(r.lastKey), SliceFactory.newUnpooled(handleEncoding)); // TODO
				r.pendingIndexEntry = false;
			}
			writeBlock(r.indexBlockBuilder, indexBlockHandle);
		}

		// Write footer
		if (ok()) {
			Footer footer = new Footer();
			footer.setMetaindexHandle(metaindexBlockHandle);
			footer.setIndexHandle(indexBlockHandle);
			ByteBuf footerEncoding = ByteBufFactory.newUnpooled();
			footer.encodeTo(footerEncoding);
			r.status = r.file.append(SliceFactory.newUnpooled(footerEncoding)); // TODO
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
		assert (!r.closed);
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
		// block_data: uint8[n]
		// type: uint8
		// crc: uint32
		assert (ok());
		Rep r = rep;
		Slice raw = block.finish();

		Slice blockContents = SliceFactory.newUnpooled();
		CompressionType type = r.options.compression;

		// TODO(postrelease): Support more compression options: zlib?
		switch (type) {
			case kNoCompression:
				blockContents = raw.clone();
				break;
	
			case kSnappyCompression: {
				if (Snappy.compress(raw.data(), 0, raw.size(), r.compressedOutput) && r.compressedOutput.size() < raw.size() - (raw.size() / 8)) {
					blockContents.init(r.compressedOutput);
				} else {
					// Snappy not supported, or compressed less than 12.5%, so just
					// store uncompressed form
					blockContents = raw.clone();
					type = CompressionType.kNoCompression;
				}
				break;
			}
		}

		writeRawBlock(blockContents, type, handle);
		r.compressedOutput.clear();
		block.reset();
	}

	void writeRawBlock(Slice blockContents, CompressionType type, BlockHandle handle) {
		Rep r = rep;
		handle.setOffset(r.offset);
		handle.setSize(blockContents.size());
		r.status = r.file.append(blockContents);
		if (r.status.ok()) {
			byte[] trailer = new byte[TableFormat.kBlockTrailerSize];
			trailer[0] = type.getType();
			Crc32C chksum = new Crc32C();
			chksum.update(blockContents.data(), blockContents.offset(), blockContents.size());
			chksum.update(trailer, 0, 1);
			long crc = chksum.getValue();
			Coding.encodeFixedNat32Long(trailer, 1, 5, Crc32C.mask(crc));
			r.status = r.file.append(SliceFactory.newUnpooled(trailer, 0, TableFormat.kBlockTrailerSize));
			if (r.status.ok()) {
				r.offset += blockContents.size() + TableFormat.kBlockTrailerSize;
			}
		}
	}
}
