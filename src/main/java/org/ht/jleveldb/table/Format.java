package org.ht.jleveldb.table;

import org.ht.jleveldb.CompressionType;
import org.ht.jleveldb.RandomAccessFile0;
import org.ht.jleveldb.ReadOptions;
import org.ht.jleveldb.Status;
import org.ht.jleveldb.util.ByteBuf;
import org.ht.jleveldb.util.Coding;
import org.ht.jleveldb.util.Crc32C;
import org.ht.jleveldb.util.Integer0;
import org.ht.jleveldb.util.Slice;
import org.ht.jleveldb.util.Snappy;

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
			assert(dst.size() == originalSize + kEncodedLength);
		}
		
		public Status decodeFrom(Slice input) {
			byte[] data = input.data();
			int magicOffset = kEncodedLength - 8; //const char* magic_ptr = input->data() + kEncodedLength - 8;
			long magic_lo = (Coding.decodeFixedNat32(data, magicOffset) & 0xffffffffL);
			long magic_hi = (Coding.decodeFixedNat32(data, magicOffset + 4) & 0xffffffffL);
			long magic = ((magic_hi << 32) | magic_lo);
			if (magic != kTableMagicNumber) {
				return Status.corruption("not an sstable (bad magic number)");
			}

			Status result = metaindexHandle.decodeFrom(input);
			if (result.ok()) {
				result = indexHandle.decodeFrom(input);
			}
			if (result.ok()) {
				// We skip over any leftover data (just padding for now) in "input"
			    int end = magicOffset + 8;
			    input.init(data, end, input.size() - 8);
			}
			return result;
		}

		// Encoded length of a Footer.  Note that the serialization of a
		// Footer will always occupy exactly this many bytes.  It consists
		// of two block handles and a magic number.
		public static final int kEncodedLength = 2 * BlockHandle.MaxEncodedLength + 8;
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
		result.data = new Slice();
		result.cachable = false;
		result.heapAllocated = false;
		
		// Read the block contents as well as the type/crc footer.
		// See table_builder.cc for the code that built this structure.
		int n = (int)handle.size();
		byte[] buf = new byte[n + kBlockTrailerSize];
		Slice contents = new Slice();
		Status s = file.read(handle.offset(), n + kBlockTrailerSize, contents, buf);
		if (!s.ok()) {
		    buf = null;
		    return s;
		}
		if (contents.size() != n + kBlockTrailerSize) {
		    buf = null;
		    return Status.corruption("truncated block read");
		}
		
		// Check the crc of the type and the block contents
		byte[] data = contents.data();    // Pointer to where Read put the data
		int offset = contents.offset;
		if (options.verifyChecksums) {
			long crc = Crc32C.unmask(Coding.decodeFixedNat32(data, offset + n + 1));
		    long actual = Crc32C.value(data, offset, n + 1);
		    if (actual != crc) {
		    	buf = null;
		    	s = Status.corruption("block checksum mismatch");
		    	return s;
		    }
		}
		
	    if (data[n] == CompressionType.kNoCompression.getType()) {
	    	if (data != buf) {
    			// File implementation gave us pointer to some other data.
    			// Use it directly under the assumption that it will be live
    			// while the file is open.
    			buf = null;
    			result.data = new Slice(data, offset, n);
    			result.heapAllocated = false;
    			result.cachable = false;  // Do not double-cache
    		} else {
    			result.data = new Slice(buf, 0, n);
    			result.heapAllocated = true;  //TODO: the logic behind this variable?
    			result.cachable = true;
    		}
    	} else if (data[n] == CompressionType.kSnappyCompression.getType()) {
    		Integer0 ulength0 = new Integer0();
	    	if (!Snappy.getUncompressedLength(data, 0, n, ulength0)) {
	    		buf = null;
	    		return Status.corruption("corrupted compressed block contents");
	    	}
	    	int ulength = ulength0.getValue();
	    	byte[] ubuf = new byte[ulength];
	    	if (!Snappy.uncompress(data, 0, n, ubuf)) {
	    		buf = null;
	    		ubuf = null;
	    		return Status.corruption("corrupted compressed block contents");
	    	}
	    	buf = null;
	    	result.data = new Slice(ubuf, 0, ulength);
	    	result.heapAllocated = true;
	    	result.cachable = true;
		} else {
    		buf = null;
    		return Status.corruption("bad block type");
	    }

		return Status.ok0();
	}
}
