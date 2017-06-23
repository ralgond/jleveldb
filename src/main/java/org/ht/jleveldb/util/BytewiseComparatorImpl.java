package org.ht.jleveldb.util;

public class BytewiseComparatorImpl extends Comparator0 {
	private static BytewiseComparatorImpl INSTANCE = new BytewiseComparatorImpl();
	
	public static BytewiseComparatorImpl getInstance() {
		return INSTANCE;
	}
	
	@Override
	public int compare(Slice a, Slice b) {
		return compare(a.data, a.offset, a.size(), b.data, b.offset, b.size());
	}

	@Override
	public int compare(byte[] a, int aoff, int asize, byte[] b, int boff, int bsize) {
		return ByteUtils.bytewiseCompare(a, aoff, asize, b, boff, bsize);
	}

	@Override
	public String name() {
		return "leveldb.BytewiseComparator";
	}

	@Override
	public void findShortestSeparator(ByteBuf start, Slice limit) {
	    // Find length of common prefix
	    int minLength = Integer.min(start.size(), limit.size());
	    int diffIndex = 0;
	    while ((diffIndex < minLength) &&
	           (start.getByte(diffIndex) == limit.getByte(diffIndex))) {
	    	diffIndex++;
	    }

	    if (diffIndex >= minLength) {
	        // Do not shorten if one string is a prefix of the other
	    } else {
	        int diffByte = (start.getByte(diffIndex) & 0xff);
	        if (diffByte < 0xff &&
	            diffByte + 1 < (limit.getByte(diffIndex) & 0xff)) {
	        	start.setByte(diffIndex, (byte)((start.getByte(diffIndex)+1)&0XFF));
	        	start.resize(diffIndex + 1);
	        	assert(compare(new Slice(start), limit) < 0);
	        }
	    }
	}

	@Override
	public void findShortSuccessor(ByteBuf key) {
	    // Find first character that can be incremented
	    int n = key.size();
	    for (int i = 0; i < n; i++) {
	    	int b = (key.getByte(i) & 0x0ff);
	    	if (b != 0xff) {
	    		key.setByte(i, (byte)(b + 1));
	    		key.resize(i+1);
	    		return;
	    	}
	    }
	    // *key is a run of 0xffs.  Leave it alone.
	}
}
