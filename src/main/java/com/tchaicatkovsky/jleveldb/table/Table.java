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

import com.tchaicatkovsky.jleveldb.Iterator0;
import com.tchaicatkovsky.jleveldb.Options;
import com.tchaicatkovsky.jleveldb.RandomAccessFile0;
import com.tchaicatkovsky.jleveldb.ReadOptions;
import com.tchaicatkovsky.jleveldb.Status;
import com.tchaicatkovsky.jleveldb.table.TableFormat.BlockContents;
import com.tchaicatkovsky.jleveldb.table.TableFormat.BlockHandle;
import com.tchaicatkovsky.jleveldb.table.TableFormat.Footer;
import com.tchaicatkovsky.jleveldb.table.TwoLevelIterator.BlockFunction;
import com.tchaicatkovsky.jleveldb.util.BytewiseComparatorImpl;
import com.tchaicatkovsky.jleveldb.util.Cache;
import com.tchaicatkovsky.jleveldb.util.Coding;
import com.tchaicatkovsky.jleveldb.util.Object0;
import com.tchaicatkovsky.jleveldb.util.Slice;
import com.tchaicatkovsky.jleveldb.util.SliceFactory;

public class Table {

	static class Rep {
		public void delete() {
			if (filter != null) {
				filter.delete();
				filter = null;
			}
			filterData = null;
			if (indexBlock != null) {
				indexBlock.delete();
				indexBlock = null;
			}
		}

		Options options;
		Status status;
		RandomAccessFile0 file;
		long cacheId;
		FilterBlockReader filter;
		byte[] filterData;

		/**
		 * Handle to Meta Index Block: saved from Footer
		 */
		BlockHandle metaindexHandle = new BlockHandle();
		Block indexBlock;
	};

	Rep rep;

	Table(Rep rep) {
		this.rep = rep;
	}

	public void delete() {
		rep.delete();
	}

	/**
	 * Convert an index iterator value (i.e., an encoded BlockHandle) into an iterator over the contents of the corresponding block.
	 * 
	 * @param arg
	 * @param options
	 * @param indexValue
	 * @return
	 */
	static Iterator0 blockReader(Object arg, ReadOptions options, Slice indexValue) {
		Table table = (Table) (arg);
		Cache blockCache = table.rep.options.blockCache;
		Block block = null;
		Cache.Handle cacheHandle = null;

		BlockHandle handle = new BlockHandle();
		Status s = handle.decodeFrom(indexValue.clone());

		// TODO(design) We intentionally allow extra stuff in indexValue so that
		// we can add more features in the future.

		if (s.ok()) {
			BlockContents contents = new BlockContents();
			if (blockCache != null) {
				byte[] cacheKeyBuffer = new byte[16];
				Coding.encodeFixedNat64(cacheKeyBuffer, 0, table.rep.cacheId);
				Coding.encodeFixedNat64(cacheKeyBuffer, 8, handle.offset());
				Slice key = SliceFactory.newUnpooled(cacheKeyBuffer, 0, 16);
				cacheHandle = blockCache.lookup(key);
				if (cacheHandle != null) {
					block = (Block) (blockCache.value(cacheHandle));
				} else {
					s = TableFormat.readBlock(table.rep.file, options, handle, contents);
					if (s.ok()) {
						block = new Block(contents);
						if (contents.cachable && options.fillCache) {
							// Add into block cache
							cacheHandle = blockCache.insert(key, block, (int) block.size(), deleteCachedBlock);
						}
					}
				}
			} else {
				s = TableFormat.readBlock(table.rep.file, options, handle, contents);
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

		@Override
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

		@Override
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
			Block block = (Block) (value);
			if (block == null)
				return;
			block.delete();
			block = null;
		}
	};

	/**
	 * Returns a new iterator over the table contents.</br>
	 * The result of newIterator() is initially invalid (caller must call one of the seek methods on the iterator before using it).
	 * 
	 * @param options
	 * @return
	 */
	public Iterator0 newIterator(ReadOptions options) {
		Iterator0 idxIter = rep.indexBlock.newIterator(rep.options.comparator);
		return TwoLevelIterator.newTwoLevelIterator(idxIter, 
				blockReaderCallback, this, options);
	}

	/**
	 * Given a key, return an approximate byte offset in the file where the data for that key begins (or would begin if the key were present in the file). The returned value is in terms of file bytes,
	 * and so includes effects like compression of the underlying data.</br>
	 * E.g., the approximate offset of the last key in the table will be close to the file length.
	 * 
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
			// key is past the last key in the file. Approximate the offset
			// by returning the offset of the metaindex block (which is
			// right near the end of the file).
			result = rep.metaindexHandle.offset();
		}
		indexIter.delete();
		indexIter = null;// delete indexIter;
		return result;
	}

	public interface HandleResult {
		void run(Object arg, Slice k, Slice v);
	}

	Slice internalKey2UserKey(Slice ikey) {
		return SliceFactory.newUnpooled(ikey.data(), ikey.offset(), ikey.size() - 8);
	}

	public Status internalGet(ReadOptions options, Slice ikey, Object arg, HandleResult handleResult) {
		Status s = Status.ok0();
		Iterator0 iiter = rep.indexBlock.newIterator(rep.options.comparator);

		iiter.seek(ikey);
		if (iiter.valid()) {
			FilterBlockReader filter = rep.filter;
			BlockHandle handle = new BlockHandle();
			if (filter != null && handle.decodeFrom(iiter.value().clone()).ok() && 
					!filter.keyMayMatch(handle.offset(), ikey)) {
				// Not found
			} else {
				Iterator0 blockIter = blockReader(this, options, iiter.value());
				blockIter.seek(ikey);
				if (blockIter.valid()) {
					handleResult.run(arg, blockIter.key(), blockIter.value());
				}
				s = blockIter.status();
				blockIter.delete();
			}
		}

		if (s.ok())
			s = iiter.status();

		iiter.delete();

		return s;
	}

	protected void readMeta(Footer footer) {
		if (rep.options.filterPolicy == null) {
			return; // Do not need any metadata
		}

		// TODO(sanjay): Skip this if footer.metaindexHandle() size indicates it is an empty block.
		ReadOptions opt = new ReadOptions();
		if (rep.options.paranoidChecks) {
			opt.verifyChecksums = true;
		}
		BlockContents contents = new BlockContents();

		if (!TableFormat.readBlock(rep.file, opt, footer.metaindexHandle(), contents).ok()) {
			// Do not propagate errors since meta info is not needed for operation
			return;
		}
		Block meta = new Block(contents);

		Iterator0 iter = meta.newIterator(BytewiseComparatorImpl.getInstance());
		String key = "filter.";
		key += (rep.options.filterPolicy.name());
		iter.seek(SliceFactory.newUnpooled(key));
		if (iter.valid() && iter.key().equals(SliceFactory.newUnpooled(key))) {
			readFilter(iter.value());
		}
		iter.delete(); // delete iter;
		meta.delete(); // delete meta;
	}

	protected void readFilter(Slice filterHandleValue) {
		Slice v = filterHandleValue.clone();
		BlockHandle filterHandle = new BlockHandle();
		if (!filterHandle.decodeFrom(v).ok()) {
			return;
		}

		// We might want to unify with readBlock() if we start
		// requiring checksum verification in Table.open.
		ReadOptions opt = new ReadOptions();
		if (rep.options.paranoidChecks) {
			opt.verifyChecksums = true;
		}
		BlockContents block = new BlockContents();

		if (!TableFormat.readBlock(rep.file, opt, filterHandle, block).ok()) {
			return;
		}
		if (block.heapAllocated) {
			rep.filterData = block.data.data(); // Will need to delete later
		}
		rep.filter = new FilterBlockReader(rep.options.filterPolicy, block.data);
	}

	public static Status open(Options options, RandomAccessFile0 file, long size, Object0<Table> table) {

		table.setValue(null);
		if (size < Footer.kEncodedLength)
			return Status.corruption("file is too short to be an sstable");

		byte footerSpace[] = new byte[Footer.kEncodedLength];
		Slice footerInput = SliceFactory.newUnpooled();
		Status s = file.read(size - Footer.kEncodedLength, Footer.kEncodedLength, footerInput, footerSpace);
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
			s = TableFormat.readBlock(file, opt, footer.indexHandle(), contents);
			if (s.ok())
				indexBlock = new Block(contents);
		}

		if (s.ok()) {
			// We've successfully read the footer and the index block: we're ready to serve requests.
			Rep rep = new Rep();
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
