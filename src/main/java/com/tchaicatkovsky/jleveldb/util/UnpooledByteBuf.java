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
package com.tchaicatkovsky.jleveldb.util;

import java.nio.BufferOverflowException;

public class UnpooledByteBuf implements ByteBuf {

	byte[] data;
	int limit;
	int offset;
	int capacity;

	public UnpooledByteBuf() {
		data = null;
		limit = 0;
		offset = 0;
		capacity = 0;
	}
	
	@Override
	final public byte[] data() {
		return data;
	}
	
	@Override
	final public int offset() {
		return offset;
	}
	
	@Override
	final public int incrOffset(int incr) {
		assert(offset+incr >= 0 && offset+incr <= limit);
		
		offset += incr;
		return offset;
	}
	
	@Override
	final public void setOffset(int offset) {
		assert(offset >= 0 & offset <= limit);
		
		this.offset = offset;
	}
	
	@Override
	final public int size() {
		return limit;
	}
	
	@Override
	final public int capacity() {
		return capacity;
	}
	
	@Override
	final public int limit() {
		return limit;
	}
	
	@Override
	final public byte getByte(int idx) {
		if (idx < 0 || idx >= limit)
			throw new BufferOverflowException();
		return data[idx];
	}
	
	@Override
	final public void setByte(int idx, byte b) {
		if (idx < 0 || idx >= limit)
			throw new BufferOverflowException();
		data[idx] = b;
	}
	
	@Override
	public void init(Slice s) {
		// Do nothing
	}
	
	@Override
	public void init(byte[] data, int offset, int size) {
		// Do nothing
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
		ByteBuf ret = new UnpooledByteBuf();
		ret.assign(data, 0, size());
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
		offset = 0;
		limit = 0;
	}
	
	@Override
	final public boolean empty() {
		return size() == 0;
	}
	
	@Override
	public void swap(ByteBuf buf0) {
		UnpooledByteBuf b = (UnpooledByteBuf)buf0;
		
		byte[] data0 = data;
		int limit0 = limit;
		int offset0 = offset;
		int capacity0 = capacity;
		
		data = b.data;
		limit = b.limit;
		offset = b.offset;
		capacity = b.capacity;
		
		b.data = data0;
		b.limit = limit0;
		b.offset = offset0;
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
		
		offset = 0;
		if (n < size()) {
			limit = n;
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
		limit = n;
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
		
		if (capacity - limit < bytes) {
			int total  = limit + bytes;
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
		UnpooledByteBuf b= (UnpooledByteBuf)o;
		
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
		
		System.arraycopy(buf, offset, data, limit, size);
		limit += size;
	}
	
	@Override
	final public void append(ByteBuf buf) {
		append(buf.data(), 0, buf.size());
	}
	
	@Override
	final public void addByte(byte b) {
		require(1);
		data[limit++] = b;
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
	final public void addFixedNat32(int value) {
		require(4);
		Coding.encodeFixedNat32(data, limit, limit+4, value);
		limit += 4;
	}
	
	@Override
	final public void addFixedNat32Long(long value) {
		require(4);
		Coding.encodeFixedNat32Long(data, limit, limit+4, value);
		limit += 4;
	}

	@Override
	final public void addFixedNat64(long value) {
		require(8);
		Coding.encodeFixedNat64(data, limit, limit+8, value);
		limit += 8;
	}

	@Override
	final public void addVarNat32(int value) {
		byte[] tmp = new byte[8]; //TODO
		int offset = Coding.encodeVarNat32(tmp, 0, 8, value);
		append(tmp, offset);
	}

	@Override
	final public void addVarNat64(long value) {
		byte[] tmp = new byte[16]; //TODO
		int offset = Coding.encodeVarNat64(tmp, 0, 16, value);
		append(tmp, offset);
	}
	

	@Override
	final public void addLengthPrefixedSlice(Slice value) {
		addVarNat32(value.size());
		append(value.data(), value.offset(), value.size());
	}
	
	@Override
	final public int readFixedNat32() {
		int ret = Coding.decodeFixedNat32(data, offset, capacity);
		offset += 4;
		return ret;
	}
	
	@Override
	final public long readFixedNat64() {
		long ret = Coding.decodeFixedNat64(data, offset, capacity);
		offset += 8;
		return ret;
	}
	
	@Override
	final public int readVarNat32() {
		return Coding.popVarNat32(this);
	}

	@Override
	final public long readVarNat64() {
		return Coding.popVarNat64(this);
	}
	
	@Override
	final public Slice readLengthPrefixedSlice() {
		int size = readVarNat32();
		Slice slice = SliceFactory.newUnpooled();
		slice.init(new byte[size], 0, size);
		System.arraycopy(data, offset, slice.data(), 0, size);
		offset += size;
		return slice;
	}
	

	
	@Override
	public long hashCode0() {
		return Hash.hash0(data, offset, size(), 301);
	}
	
	@Override
	public void removePrefix(int n) {
		assert(n <= size());
		offset +=n ;
	}
	
	@Override
	final public int compare(Slice b) {
		return ByteUtils.bytewiseCompare(data, offset, size(), b.data(), b.offset(), b.size());
	}
}
