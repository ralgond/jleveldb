package org.ht.jleveldb.util;

import java.nio.BufferUnderflowException;

public class Slice {
	public byte[] data;
	public int offset;
	public int limit;
	
	public Slice() {
		
	}

	public Slice(Slice s) {
		init(s);
	}
	
	public Slice(String s) {
		byte[] b = s.getBytes();
		init(b, 0, b.length);
	}
	
	public Slice(ByteBuf buf) {
		init(buf);
	}
	
	public Slice(byte[] data, int offset, int size) {
		init(data, offset, size);
	}
	
	public void init(byte[] data, int offset, int size) {
		this.data = data;
		this.offset = offset;
		this.limit = offset + size;
	}
	
	public void init(Slice s) {
		init(s.data, s.offset, s.size());
	}
	
	public void init(ByteBuf buf) {
		init(buf.data(), 0, buf.size());
	}
	
	public byte getByte(int idx) {
		if (idx < 0 || idx >= size())
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
		return new String(data, offset, size());
	}
	
	public void clear() {
		data = null;
		offset = 0;
		limit = 0;
	}
	
	@Override
	public Slice clone() {
		return new Slice(this);
	}
	
	final public int compare(Slice b) {
		return ByteUtils.bytewiseCompare(data, offset, size(), b.data, b.offset, b.size());
	}
	
	// Drop the first "n" bytes from this slice.
	public void removePrefix(int n) {
		assert(n <= size());
	    offset += n;
	}
	
	@Override
	public boolean equals(Object o) {
		Slice s = (Slice)o;
		if (size() == 0 && size() == size())
			return true;
		
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
	
	@Override
	public String toString() {
		return encodeToString();
	}
}
