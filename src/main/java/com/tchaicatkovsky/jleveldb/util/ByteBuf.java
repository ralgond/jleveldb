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

public interface ByteBuf extends Slice {
	
	int capacity();
	
	
	void resize(int bytes);
	
	void resize(int bytes, byte value);
	
	void require(int bytes);
	
	void swap(ByteBuf buf);
	
	byte getByte(int idx);
	
	void setByte(int idx, byte b);
	
	ByteBuf clone();

	
	public String encodeToString();
	
	
	
	void assign(byte[] data, int size);
	
	void assign(byte[] data, int offset, int size);
	
	void assign(String s);
	
	void assign(ByteBuf buf);
	

	
	void append(byte[] buf, int size);
	
	void append(byte[] buf, int offset, int size);
	
	void append(ByteBuf buf);
	
	void addByte(byte b);
	
	/**
	 * append 32bit fixed natural number.
	 * @param value
	 */
	public void writeFixedNat32(int value);
	
	/**
	 * append 32bit fixed natural number.
	 * @param value
	 */
	public void writeFixedNat32Long(long value);
	
	/**
	 * append 64bit fixed natural number.
	 * @param value
	 */
	public void writeFixedNat64(long value);
	
	/**
	 * append 32bit var natural number.
	 * @param value
	 * @throws Exception
	 */
	public void writeVarNat32(int value);
	
	/**
	 * append 32bit var natural number.
	 * @param value
	 */
	public void writeVarNat64(long value);
	
	/**
	 * append slice.
	 * @param value
	 */
	public void writeLengthPrefixedSlice(Slice value);
	
	/**
	 * read 32bit fixed natural number.
	 * @return
	 */
	public int readFixedNat32();
	
	/**
	 * read 64bit fixed natural number.
	 * @return
	 */
	public long readFixedNat64();
	
	/**
	 * read 32bit var natural number.
	 * @return
	 */
	public int readVarNat32();
	
	/**
	 * read 64bit var natural number.
	 * @return
	 */
	public long readVarNat64();
	
	/**
	 * read slice.
	 * @param value
	 */
	public Slice readLengthPrefixedSlice();
}
