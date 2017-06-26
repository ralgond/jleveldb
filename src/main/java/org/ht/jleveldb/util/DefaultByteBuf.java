package org.ht.jleveldb.util;

import java.nio.BufferOverflowException;

public class DefaultByteBuf implements ByteBuf {

	byte[] data;
	int writeIndex;
	int readIndex;
	int capacity;
	Slice readSlice;

	public DefaultByteBuf() {
		data = null;
		writeIndex = 0;
		readIndex = 0;
		capacity = 0;
		readSlice = new Slice();
	}
	
	@Override
	final public byte[] data() {
		return data;
	}
	
	@Override
	final public int capacity() {
		return capacity;
	}
	
	@Override
	final public int position() {
		return writeIndex;
	}
	
	@Override
	public void init(byte[] data, int size) {
		this.data = data;
		writeIndex = 0;
		readIndex = 0;
		capacity = size;
	}
	
	@Override
	final public byte getByte(int idx) {
		if (idx < 0 || idx >= writeIndex)
			throw new BufferOverflowException();
		return data[idx];
	}
	
	@Override
	final public void setByte(int idx, byte b) {
		if (idx < 0 || idx >= writeIndex)
			throw new BufferOverflowException();
		data[idx] = b;
	}
	
	@Override
	public void assign(byte[] data, int size) {
		assign(data, 0, size);
	}
	
	@Override
	public void assign(byte[] data, int offset, int size) {
		clear();
		append(data, offset, size);
	}

	@Override
	public ByteBuf clone() {
		ByteBuf ret = new DefaultByteBuf();
		ret.assign(data, 0, size());
		return ret;
	}
	
	@Override
	public byte[] copyData() {
		byte[] ret = new byte[writeIndex];
		System.arraycopy(data, 0, ret, 0, writeIndex);
		return ret;
	}
	
	@Override
	final public String encodeToString() {
		if (data == null || size() == 0)
			return "";
		
		return new String(data, 0, size());
	}

	@Override
	final public void clear() {
		writeIndex = 0;
		readIndex = 0;
	}

	@Override
	final public boolean isDirect() {
		return false;
	}

	@Override
	final public int size() {
		return writeIndex;
	}
	
	@Override
	final public boolean empty() {
		return size() == 0;
	}
	
	@Override
	public void swap(ByteBuf buf0) {
		DefaultByteBuf b = (DefaultByteBuf)buf0;
		
		byte[] data0 = data;
		int writeIndex0 = writeIndex;
		int readIndex0 = readIndex;
		int capacity0 = capacity;
		
		data = b.data;
		writeIndex = b.writeIndex;
		readIndex = b.readIndex;
		capacity = b.capacity;
		
		b.data = data0;
		b.writeIndex = writeIndex0;
		b.readIndex = readIndex0;
		b.capacity = capacity0;
	}
	
	@Override
	public void resize(int n) {
		resize(n, (byte)0);
	}
	
	@Override
	public void resize(int n, byte value) {
		if (n < 0)
			return;
		
		readIndex = 0;
		if (n < size()) {
			writeIndex = n;
			return;
		}
		
		int oldSize = size();
		int newcapacity = calculateCapacity(n);
		if (newcapacity > capacity()) {
			byte[] newData = new byte[newcapacity];
			if (size() > 0)
				System.arraycopy(data, 0, newData, 0, size());
			data = newData;
			capacity = newcapacity;
		}
		
		for (int i = oldSize; i < n; i++)
			data[i] = value;
		writeIndex = n;
	}
	
	final int calculateCapacity(int size) {
		int newcapacity = 1;
		while (newcapacity < size)
			newcapacity = newcapacity << 1;
		if (newcapacity < 16)
			newcapacity = 16;
		return newcapacity;
	}

	@Override
	final public void require(int bytes) {
		if (bytes <= 0)
			return;
		
		if (capacity - writeIndex < bytes) {
			int total  = writeIndex + bytes;
			int newcapacity = calculateCapacity(total);
			byte[] newData = new byte[newcapacity];
			if (size() > 0)
				System.arraycopy(data, 0, newData, 0, size());
			data = newData;
			capacity = newcapacity;
		}
	}
	
	@Override
	public boolean equals(Object o) {
		DefaultByteBuf b= (DefaultByteBuf)o;
		
		if (size() == 0 && size() == b.size())
			return true;
		
		if (data != null && b.data != null && size() == b.size()) {
			int size = size();
			byte[] bdata = b.data;
			for (int i = 0; i < size; i++) {
				if (data[i] != bdata[i])
					return false;
			}
			return true;
		} else {
			return false;
		}
	}
	
	@Override
	final public void append(byte[] buf, int size) {
		append(buf, 0 ,size);
	}
	
	@Override
	final public void append(byte[] buf, int offset, int size) {
		require(size);
		if (size <= 0)
			return;
		
		System.arraycopy(buf, offset, data, writeIndex, size);
		writeIndex += size;
	}
	
	@Override
	final public void append(ByteBuf buf) {
		append(buf.data(), 0, buf.size());
	}
	
	@Override
	public void assign(String s) {
		byte[] b = s.getBytes();
		assign(b, 0, b.length);
	}
	
	@Override
	public void assign(ByteBuf buf) {
		assign(buf.data(), 0, buf.size());
	}

	@Override
	final public void writeFixedNat32(int value) {
		require(4);
		Coding.encodeFixedNat32(data, writeIndex, writeIndex+4, value);
		writeIndex += 4;
	}
	
	@Override
	final public void writeFixedNat32Long(long value) {
		require(4);
		Coding.encodeFixedNat32Long(data, writeIndex, writeIndex+4, value);
		writeIndex += 4;
	}

	@Override
	final public void writeFixedNat64(long value) {
		require(8);
		Coding.encodeFixedNat64(data, writeIndex, writeIndex+8, value);
		writeIndex += 8;
	}

	@Override
	final public void writeVarNat32(int value) {
		byte[] tmp = new byte[8];
		int offset = Coding.encodeVarNat32(tmp, 0, 8, value);
		append(tmp, offset);
	}

	@Override
	final public void writeVarNat64(long value) {
		byte[] tmp = new byte[16];
		int offset = Coding.encodeVarNat64(tmp, 0, 16, value);
		append(tmp, offset);
	}
	

	@Override
	final public void writeLengthPrefixedSlice(Slice value) {
		writeVarNat32(value.size());
		append(value.data, value.offset, value.size());
	}
	
	@Override
	final public int readFixedNat32() {
		int ret = Coding.decodeFixedNat32(data, readIndex, capacity);
		readIndex += 4;
		return ret;
	}
	
	@Override
	final public long readFixedNat64() {
		long ret = Coding.decodeFixedNat64(data, readIndex, capacity);
		readIndex += 8;
		return ret;
	}
	
	@Override
	final public int readVarNat32() {
		readSlice.clear();
		readSlice.init(data, readIndex, capacity);
		int ret = Coding.getVarNat32Ptr(readSlice);
		readIndex = readSlice.offset;
		return ret;
	}

	@Override
	final public long readVarNat64() {
		readSlice.clear();
		readSlice.init(data, readIndex, capacity);
		long ret = Coding.getVarNat64Ptr(readSlice);
		readIndex = readSlice.offset;
		return ret;
	}
	
	@Override
	final public Slice readLengthPrefixedSlice() {
		int size = readVarNat32();
		Slice slice = new Slice();
		slice.init(new byte[size], 0, size);
		System.arraycopy(data, readIndex, slice.data, 0, size);
		readIndex += size;
		return slice;
	}
	
	@Override
	final public void addByte(byte b) {
		require(1);
		data[writeIndex++] = b;
	}
}
