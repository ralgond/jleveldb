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
import com.tchaicatkovsky.jleveldb.util.Slice;
import com.tchaicatkovsky.jleveldb.util.SliceFactory;

//import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestSlice {
	@Test
	public void testEquals() {
		byte[] data = new byte[]{1,2,3};
		Slice s1 = SliceFactory.newUnpooled(data, 0, data.length);
		Slice s2 = SliceFactory.newUnpooled(data, 0, data.length);
		
		assertTrue(s1.equals(s2));
		
		byte[] data2 = new byte[]{1,2,3,1,2,3};
		Slice s3 = SliceFactory.newUnpooled(data2, 3, 3);
		assertTrue(s1.equals(s3));
		
		Slice s4 = SliceFactory.newUnpooled(data2, 0, data2.length);
		assertTrue(!s1.equals(s4));
	}
	
	@Test
	public void testWithByteBuf() {
		byte[] data = new byte[]{1,2,3};
		Slice s1 = SliceFactory.newUnpooled(data, 0, data.length);
		
		ByteBuf buf = ByteBufFactory.newUnpooled();
		buf.addByte((byte)1);
		buf.addByte((byte)2);
		buf.addByte((byte)3);
		
		assertTrue(s1.equals(SliceFactory.newUnpooled(buf)));
	}
	
	@Test
	public void testWithString() {
		Slice s1 = SliceFactory.newUnpooled("abc123");
		s1.removePrefix(3);
		assertTrue(s1.encodeToString().equals("123"));
	}
	
	@Test
	public void testCompare() {
		Slice s1 = SliceFactory.newUnpooled("123");
		Slice s2 = SliceFactory.newUnpooled("234");
		Slice s3 = SliceFactory.newUnpooled("123");
		Slice s4 = SliceFactory.newUnpooled("1");
		
		assertTrue(s1.compare(s2) < 0);
		assertTrue(s1.compare(s3) == 0);
		assertTrue(s2.compare(s1) > 0);
		
		assertTrue(s4.compare(s1) < 0);
	}
}
