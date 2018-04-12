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

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.tchaicatkovsky.jleveldb.db.format.InternalKeyComparator;
import com.tchaicatkovsky.jleveldb.util.ByteBuf;
import com.tchaicatkovsky.jleveldb.util.ByteBufFactory;
import com.tchaicatkovsky.jleveldb.util.BytewiseComparatorImpl;
import com.tchaicatkovsky.jleveldb.util.Slice;
import com.tchaicatkovsky.jleveldb.util.SliceFactory;

public class TestComparator0 {
	@Test
	public void testBytewiseComparatorImpl() {
		Slice a = SliceFactory.newUnpooled("abc");
		Slice b = SliceFactory.newUnpooled("9999");
		int ret = BytewiseComparatorImpl.getInstance().compare(a, b);
		assertTrue(ret > 0);
	}
	
	@Test
	public void testBytewiseComparatorImpl1() {
		byte[] abuf = new byte[]{(byte)0x0a, (byte)0xfd, (byte)0xfd, (byte)0x01, (byte)0x03, 
								 (byte)0x00, (byte)0x00, (byte)0x00, (byte)0x00, (byte)0x00};
		ByteBuf a = ByteBufFactory.newUnpooled(abuf, abuf.length);
		

		byte[] bbuf = new byte[]{(byte)0x09, (byte)0xfd, (byte)0x01, (byte)0x02,
								 (byte)0x00, (byte)0x00, (byte)0x00, (byte)0x00, (byte)0x00};
		ByteBuf b = ByteBufFactory.newUnpooled(bbuf, bbuf.length);
		
		InternalKeyComparator icmp = new InternalKeyComparator(BytewiseComparatorImpl.getInstance());
		int ret = icmp.compare(a, b);
		assertTrue(ret > 0);
	}
}
