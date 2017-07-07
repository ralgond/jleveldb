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

import java.nio.BufferUnderflowException;

public class UnpooledSlice implements Slice {
	public byte[] data;
	public int offset;
	public int limit;
	
	public UnpooledSlice() {
		
	}

	public UnpooledSlice(Slice s) {
		init(s);
	}
	
	public UnpooledSlice(String s) {
		byte[] b = s.getBytes();
		init(b, 0, b.length);
	}
	
	public UnpooledSlice(ByteBuf buf) {
		init(buf);
	}
	
	public UnpooledSlice(byte[] data, int offset, int size) {
		init(data, offset, size);
	}
	
	@Override
	public void init(byte[] data, int offset, int size) {
		this.data = data;
		this.offset = offset;
		this.limit = offset + size;
	}
	
	public void init(Slice s) {
		init(s.data(), s.offset(), s.size());
	}
	
	public void init(ByteBuf b) {
		init(b.data(), b.offset(), b.size());
	}
	
	public void init(UnpooledSlice s) {
		init(s.data(), s.offset(), s.size());
	}
	
	@Override
	public byte getByte(int idx) {
		if (idx < 0 || idx >= size())
			throw new BufferUnderflowException();
		
		return data[offset+idx];
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
		offset += incr;
		return offset;
	}
	
	@Override
	final public void setOffset(int offset) {
		this.offset = offset;
	}
	
	@Override
	final public int limit() {
		return limit;
	}
	
	@Override
	final public int size() {
		return limit - offset;
	}
	
	@Override
	final public boolean empty() {
		return size() == 0;
	}
	
	@Override
	public String encodeToString() {
		if (data == null || size() == 0)
			return "";
		return new String(data, offset, size());
	}
	
	@Override
	public String escapeString() {
		return Strings.escapeString(this);
	}
	
	@Override
	public void clear() {
		data = null;
		offset = 0;
		limit = 0;
	}
	
	@Override
	public Slice clone() {
		return SliceFactory.newUnpooled(this);
	}
	
	@Override
	final public int compare(Slice s) {
		return ByteUtils.bytewiseCompare(data, offset, size(), s.data(), s.offset(), s.size());
	}
	
	@Override
	final public int compare(ByteBuf b) {
		return ByteUtils.bytewiseCompare(data, offset, size(), b.data(), b.offset(), b.size());
	}
	
	// Drop the first "n" bytes from this slice.
	@Override
	public void removePrefix(int n) {
		assert(n <= size());
	    offset += n;
	}
	
	@Override
	public boolean equals(Object o) {
		Slice s = (Slice)o;
		if (size() == 0 && size() == size())
			return true;
		
		if (data != null && s.data() != null && size() == s.size()) {
			int size = size();
			int soffset = s.offset();
			byte[] sdata = s.data();
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
	public long hashCode0() {
		return Hash.hash0(data, offset, size(), 301);
	}
	
	@Override
	public String toString() {
		return encodeToString();
	}
	
	
	
	@Override
	public int readFixedNat32() {
		int ret = Coding.decodeFixedNat32(data, offset, limit);
		offset += 4;
		return ret;
	}
	
	@Override
	public long readFixedNat64() {
		long ret = Coding.decodeFixedNat64(data, offset, limit);
		offset += 8;
		return ret;
	}
	
	@Override
	public int readVarNat32() {
		return Coding.popVarNat32(this);
	}

	@Override
	public long readVarNat64() {
		return Coding.popVarNat64(this);
	}
	
	@Override
	public Slice readLengthPrefixedSlice() {
		int size = readVarNat32();
		Slice slice = SliceFactory.newUnpooled();
		slice.init(new byte[size], 0, size);
		System.arraycopy(data, offset, slice.data(), 0, size);
		offset += size;
		return slice;
	}
}
