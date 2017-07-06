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

public interface Slice {
	byte[] data();
	
	int offset();
	
	int limit();
	
	int size();
	
	int incrOffset(int inc);
	
	void setOffset(int offset);
	
	void init(Slice s);
	
	void init(ByteBuf b);
	
	void init(byte[] data, int offset, int size);
	
	byte getByte(int idx);
	
	boolean empty();
	
	void clear();
	
	String encodeToString();
	
	int compare(Slice b);
	
	void removePrefix(int n);
	
	long hashCode0();
	
	Slice clone();
	
	/**
	 * read 32bit fixed natural number.
	 * @return
	 */
	int readFixedNat32();
	
	/**
	 * read 64bit fixed natural number.
	 * @return
	 */
	long readFixedNat64();
	
	/**
	 * read 32bit var natural number.
	 * @return
	 */
	int readVarNat32();
	
	/**
	 * read 64bit var natural number.
	 * @return
	 */
	long readVarNat64();
	
	/**
	 * read slice.
	 * @param value
	 */
	Slice readLengthPrefixedSlice();
}
