package org.ht.jleveldb.table;

import org.ht.jleveldb.RandomAccessFile0;
import org.ht.jleveldb.ReadOptions;
import org.ht.jleveldb.Status;
import org.ht.jleveldb.util.ByteBuf;
import org.ht.jleveldb.util.Coding;
import org.ht.jleveldb.util.Slice;

public class Format {
	//BlockHandle is a pointer to the extent of a file that stores a data
	//block or a meta block.
	public static class BlockHandle {
		// The offset of the block in the file.
		final public long offset() { 
			return offset; 
		}
		
		final public void setOffset(long offset) { 
			this.offset = offset; 
		}

		// The size of the stored block
		final public long size() { 
			return size; 
		}
		
		final public void setSize(long size) { 
			this.size = size; 
		}

		public void encodeTo(ByteBuf dst) {
			// Sanity check that all fields have been set
			//assert(offset != ~static_cast<uint64_t>(0));
			//assert(size != ~static_cast<uint64_t>(0));
			dst.writeVarNat64(offset);
			dst.writeVarNat64(size);
		}
		
		public Status decodeFrom(Slice input) {
			if ((offset = Coding.getVarNat64(input)) > 0 && (size = Coding.getVarNat64(input)) > 0) {
				return Status.ok0();
			} else {
				return Status.corruption("bad block handle");
			}
		}

		// Maximum encoding length of a BlockHandle
		public static final int MaxEncodedLength = 10 + 10;

		long offset = Long.MAX_VALUE;
		long size = Long.MAX_VALUE;
	}
	
	public static class Footer {
		BlockHandle metaindexHandle;
		BlockHandle indexHandle;
		
		// The block handle for the metaindex block of the table
		final public BlockHandle metaindexHandle() { 
			return metaindexHandle; 
		}
		
		final public void setMetaindexHandle(BlockHandle h) {
			metaindexHandle = h; 
		}

		// The block handle for the index block of the table
		final public BlockHandle indexHandle() {
		    return indexHandle;
		}
		
		final public void setIndexHandle(BlockHandle h) {
		    indexHandle = h;
		}

		public void encodeTo(ByteBuf dst) {
			int originalSize = dst.size();
			metaindexHandle.encodeTo(dst);
			indexHandle.encodeTo(dst);
			dst.resize(2 * BlockHandle.MaxEncodedLength);  // Padding
			dst.writeFixedNat32Long((kTableMagicNumber & 0xffffffffL));
			dst.writeFixedNat32Long((kTableMagicNumber >> 32));
			assert(dst.size() == originalSize + EncodedLength);
		}
		
		public Status decodeFrom(Slice input) {
			const char* magic_ptr = input->data() + kEncodedLength - 8;
			const uint32_t magic_lo = DecodeFixed32(magic_ptr);
			const uint32_t magic_hi = DecodeFixed32(magic_ptr + 4);
			const uint64_t magic = ((static_cast<uint64_t>(magic_hi) << 32) |
			                          (static_cast<uint64_t>(magic_lo)));
			if (magic != kTableMagicNumber) {
				return Status.corruption("not an sstable (bad magic number)");
			}

			Status result = metaindexHandle.decodeFrom(input);
			if (result.ok()) {
				result = indexHandle.decodeFrom(input);
			}
			if (result.ok()) {
				// We skip over any leftover data (just padding for now) in "input"
			    const char* end = magic_ptr + 8;
			    *input = Slice(end, input->data() + input->size() - end);
			}
			return result;
		}

		// Encoded length of a Footer.  Note that the serialization of a
		// Footer will always occupy exactly this many bytes.  It consists
		// of two block handles and a magic number.
		public static final int EncodedLength = 2 * BlockHandle.MaxEncodedLength + 8;
	}

	// kTableMagicNumber was picked by running
	//  echo http://code.google.com/p/leveldb/ | sha1sum
	//and taking the leading 64 bits.
	public static final long kTableMagicNumber = 0xdb4775248b80fb57L;
	
	// 1-byte type + 32-bit crc
	public static final int kBlockTrailerSize = 5;
	
	public static class BlockContents {
		public Slice data;           // Actual contents of data
		public boolean cachable;        // True iff data can be cached
		public boolean heapAllocated;   // True iff caller should delete[] data.data()
	}

	// Read the block identified by "handle" from "file".  On failure
	// return non-OK.  On success fill *result and return OK.
	public static Status readBlock(RandomAccessFile0 file,
            ReadOptions options,
            BlockHandle handle,
            BlockContents result) {
		//TODO
		return null;
	}
}
