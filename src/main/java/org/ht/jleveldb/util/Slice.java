package org.ht.jleveldb.util;

import java.nio.BufferUnderflowException;

public class Slice {
	public byte[] data;
	public int offset;
	public int limit;
	
	public Slice() {
		
	}

	public Slice(ByteBuf buf) {
		init(buf.data(), 0, buf.size());
	}
	
	public Slice(byte[] data, int offset, int size) {
		init(data, offset, size);
	}
	
	public void init(byte[] data, int offset, int size) {
		this.data = data;
		this.offset = offset;
		this.limit = offset + size;
	}
	
	public byte getByte(int idx) {
		if (idx < offset || idx >= limit)
			throw new BufferUnderflowException();
		
		return data[offset+idx];
	}
	
	final public int size() {
		return limit - offset;
	}
	
	final public boolean empty() {
		return size() == 0;
	}
	
	final public byte[] data() {
		return data;
	}
	
	final public int offset() {
		return offset;
	}
	
	public String encodeToString() {
		return null; //TODO
	}
	
	public void clear() {
		//TODO
	}
	
	final public static int memcmp(byte[] a, int aoff, byte[] b, int boff, int size) {
		for (int i = 0; i < size; i++) {
			if (a[aoff+i] < b[boff+i])
				return -1;
			else if (a[aoff+i] > b[boff+i])
				return 1;
		}
		return 0;
	}
	
	final public int compare(Slice b) {
		int minLen = (size() < b.size()) ? size() : b.size();
		int r = memcmp(data, offset, b.data, b.offset, minLen);
		if (r == 0) {
		    if (size() < b.size()) r = -1;
		    else if (size() > b.size()) r = +1;
		}
		return r;
	}
	
	// Drop the first "n" bytes from this slice.
	public void removePrefix(int n) {
		assert(n <= size());
	    offset += n;
	}
	
	@Override
	public boolean equals(Object o) {
		Slice s = (Slice)o;
		if (data != null && s.data != null && size() == s.size()) {
			int size = size();
			int soffset = s.offset;
			byte[] sdata = s.data;
			for (int i = 0; i < size; i++) {
				if (data[offset+i] != sdata[soffset+i])
					return false;
			}
			return true;
		} else {
			return false;
		}
	}
	
	@Override
	public int hashCode() {
		if (data == null)
            return 0;

        int result = 1;
        for (int i = offset; i < limit; i++) {
        	long element = data[i];
        	int elementHash = (int)(element ^ (element >>> 32));
        	result = 31 * result + elementHash;
        }

        return result;
	}
}
