/**
 * Copyright (c) 2017-2018, Teng Huang <ht201509 at 163 dot com>
 * All rights reserved.
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tchaicatkovsky.jleveldb.test;

import org.junit.Test;

import com.tchaicatkovsky.jleveldb.util.ByteBuf;
import com.tchaicatkovsky.jleveldb.util.ByteBufFactory;
import com.tchaicatkovsky.jleveldb.util.UnpooledByteBuf;
import com.tchaicatkovsky.jleveldb.util.ReflectionUtil;
import com.tchaicatkovsky.jleveldb.util.Slice;
import com.tchaicatkovsky.jleveldb.util.SliceFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestByteBuf {
	@Test
	public void testRequire() throws Exception {
		UnpooledByteBuf buf = (UnpooledByteBuf)ByteBufFactory.newUnpooled(); 
		
		assertTrue(buf.capacity() == 0);
		
		buf.require(1);
		assertTrue(buf.capacity() == 16);
		
		buf.require(2);
		assertTrue(buf.capacity() == 16);
		
		buf.require(16);
		assertTrue(buf.capacity() == 16);
		
		buf.require(17);
		assertTrue(buf.capacity() == 32);
		
		ReflectionUtil.setValue(buf, "size", buf.offset()+16);
		buf.require(17);
		assertTrue(buf.capacity() == 64);
	}
	
	@Test
	public void testAppend() {
		int array_size = 127;
		
		byte[] a = new byte[array_size];
		for (int i = 0; i < array_size; i++)
			a[i] = (byte)i;
		
		for (int copy_len = 1; copy_len <= 127; copy_len++) {
			UnpooledByteBuf buf1 = (UnpooledByteBuf)ByteBufFactory.newUnpooled();
			int offset = 0;
			int len = copy_len;
			while (offset < a.length) {
				len = ((a.length - offset < len) ? a.length - offset : len);
				buf1.append(a, offset, len);
				offset += len;
			}
			
			assertTrue(buf1.capacity() == 128);
			assertTrue(buf1.data()[array_size] == 0);
			
			for (int i = 0; i < a.length; i++) {
				assertTrue(buf1.data()[i] == a[i]);
			}
		}
	}
	
	static Slice S0(ByteBuf b) {
		return SliceFactory.newUnpooled(b);
	}
	
	@Test
	public void testFixedCoding01() throws Exception {
		UnpooledByteBuf buf = (UnpooledByteBuf)ByteBufFactory.newUnpooled();
		Slice s = null;
		
		buf.addFixedNat32(0);
		buf.addFixedNat32(Short.MAX_VALUE);
		buf.addFixedNat32(Integer.MAX_VALUE);
		
		s = S0(buf);
		
		assertTrue(s.readFixedNat32() == 0);
		assertTrue(s.readFixedNat32() == Short.MAX_VALUE);
		assertTrue(s.readFixedNat32() == Integer.MAX_VALUE);
		
		buf.clear();
		
		buf.addFixedNat64(0);
		buf.addFixedNat64(Short.MAX_VALUE);
		buf.addFixedNat64(Integer.MAX_VALUE);
		buf.addFixedNat64(Long.MAX_VALUE);

		s = S0(buf);
		
		assertTrue(s.readFixedNat64() == 0);
		assertTrue(s.readFixedNat64() == Short.MAX_VALUE);
		assertTrue(s.readFixedNat64() == Integer.MAX_VALUE);
		assertTrue(s.readFixedNat64() == Long.MAX_VALUE);
		
		buf.clear();
		
		buf.addFixedNat64(Long.MAX_VALUE);
		buf.addFixedNat32(0);
		buf.addFixedNat64(Integer.MAX_VALUE);
		buf.addFixedNat32(Short.MAX_VALUE);
		buf.addFixedNat64(Short.MAX_VALUE);
		buf.addFixedNat32(Integer.MAX_VALUE);
		buf.addFixedNat64(0);
		
		s = S0(buf);
		
		assertTrue(s.readFixedNat64() == Long.MAX_VALUE);
		assertTrue(s.readFixedNat32() == 0);
		assertTrue(s.readFixedNat64() == Integer.MAX_VALUE);
		assertTrue(s.readFixedNat32() == Short.MAX_VALUE);
		assertTrue(s.readFixedNat64() == Short.MAX_VALUE);
		assertTrue(s.readFixedNat32() == Integer.MAX_VALUE);
		assertTrue(s.readFixedNat64() == 0);
	}
	

	
	@Test
	public void testVarCoding01() throws Exception {
		UnpooledByteBuf buf = (UnpooledByteBuf)ByteBufFactory.newUnpooled();
		Slice s = null;
		
		buf.addVarNat32(0);
		buf.addVarNat32(Short.MAX_VALUE);
		buf.addVarNat32(Integer.MAX_VALUE);
		
		
		s = S0(buf);
		
		assertTrue(s.readVarNat32() == 0);
		assertTrue(s.readVarNat32() == Short.MAX_VALUE);
		assertTrue(s.readVarNat32() == Integer.MAX_VALUE);
		
		buf.clear();
		
		buf.addVarNat64(0);
		buf.addVarNat64(Short.MAX_VALUE);
		buf.addVarNat64(Integer.MAX_VALUE);
		buf.addVarNat64(Long.MAX_VALUE);

		s = S0(buf);
		
		assertTrue(s.readVarNat64() == 0);
		assertTrue(s.readVarNat64() == Short.MAX_VALUE);
		assertTrue(s.readVarNat64() == Integer.MAX_VALUE);
		assertTrue(s.readVarNat64() == Long.MAX_VALUE);
		
		buf.clear();
			
		buf.addVarNat64(Long.MAX_VALUE);
		buf.addVarNat32(0);
		buf.addVarNat64(Integer.MAX_VALUE);
		buf.addVarNat32(Short.MAX_VALUE);
		buf.addVarNat64(Short.MAX_VALUE);
		buf.addVarNat32(Integer.MAX_VALUE);
		buf.addVarNat64(0);
		
		s = S0(buf);
		
		assertTrue(s.readVarNat64() == Long.MAX_VALUE);
		assertTrue(s.readVarNat32() == 0);
		assertTrue(s.readVarNat64() == Integer.MAX_VALUE);
		assertTrue(s.readVarNat32() == Short.MAX_VALUE);
		assertTrue(s.readVarNat64() == Short.MAX_VALUE);
		assertTrue(s.readVarNat32() == Integer.MAX_VALUE);
		assertTrue(s.readVarNat64() == 0);
	}
	
	@Test
	public void testResize() {
		ByteBuf buf = ByteBufFactory.newUnpooled();
		Slice s = SliceFactory.newUnpooled("123456");
		buf.append(s.data(), s.offset(), s.size());
		
		buf.resize(3);
		assertEquals("123", buf.encodeToString());
		
		buf.resize(5, (byte)0x30);
		assertEquals("12300", buf.encodeToString());
	}
}
