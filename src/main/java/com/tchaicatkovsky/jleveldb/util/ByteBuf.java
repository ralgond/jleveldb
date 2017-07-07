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

public interface ByteBuf {
	
	byte[] data();
	
	int offset();
	
	int endOffset();
	
	/**
	 * @return
	 */
	int size();
	
	int capacity();
	
	/**
	 * call resize(bytes, (byte)0x00);
	 * @param bytes
	 */
	void resize(int bytes);
	
	void resize(int bytes, byte initialValue);
	
	void require(int bytes);
	
	void swap(ByteBuf buf);
	
	/**
	 * return data[offset+idx]
	 * @param idx
	 * @return
	 */
	byte getByte(int idx);
	
	/**
	 * data[offset+idx] = b;
	 * @param idx
	 * @param b
	 */
	void setByte(int idx, byte b);
	
	ByteBuf clone();
	
	void clear();
	
	boolean empty();

	String encodeToString();
	
	String escapeString();
	
	long hashCode0();
	
	int compare(Slice slice);
	
	int compare(ByteBuf b);
	
	void assign(byte[] data, int size);
	
	void assign(byte[] data, int offset, int size);
	
	void assign(String s);
	
	void assign(ByteBuf buf);
	
	void assign(Slice slice);

	
	void append(byte[] buf, int size);
	
	void append(byte[] buf, int offset, int size);
	
	void append(String s);
	
	void append(ByteBuf buf);
	
	void append(Slice slice);
	
	void addByte(byte b);
	
	/**
	 * append 32bit fixed natural number.
	 * @param value
	 */
	public void addFixedNat32(int value);
	
	/**
	 * append 32bit fixed natural number.
	 * @param value
	 */
	public void addFixedNat32Long(long value);
	
	/**
	 * append 64bit fixed natural number.
	 * @param value
	 */
	public void addFixedNat64(long value);
	
	/**
	 * append 32bit var natural number.
	 * @param value
	 * @throws Exception
	 */
	public void addVarNat32(int value);
	
	/**
	 * append 32bit var natural number.
	 * @param value
	 */
	public void addVarNat64(long value);
	
	/**
	 * append slice.
	 * @param value
	 */
	public void addLengthPrefixedSlice(Slice value);
}
