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

import com.tchaicatkovsky.jleveldb.util.Crc32C;

import static org.junit.Assert.assertEquals;
//import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

public class TestCrc32C {
	@Test
	public void test01() {
		Crc32C crc = new Crc32C();
		
		byte[] data = new byte[127];
		for (int i = 0; i < data.length; i++) {
			data[i] = (byte)i;
		}
		
		crc.update(data, 0, data.length);
		long ret1 = crc.getValue();
		
		Crc32C crc2 = new Crc32C();
		crc2.update(data, 0, 50);
		long ret2_1 = crc2.getValue();
		
		Crc32C crc3 = new Crc32C();
		crc3.setValue(ret2_1);
		crc3.update(data, 50, data.length - 50);
		long ret2 = crc3.getValue();
		
		assertEquals(ret1, ret2);
	}
	
	@Test
	public void testStandardResults() {
		// From rfc3720 section B.4.
		byte[] buf = new byte[32];
		assertEquals(0x8a9136aaL, Crc32C.value(buf, 0, buf.length));
		
		Arrays.fill(buf, (byte)0xff);
		assertEquals(0x62a8ab43L, Crc32C.value(buf, 0, buf.length));
		
		for (int i = 0; i < buf.length; i++) {
		    buf[i] = (byte)(i&0xFF);
		}
		assertEquals(0x46dd794e, Crc32C.value(buf, 0, buf.length));
		
		for (int i = 0; i < 32; i++) {
		    buf[i] = (byte)((31 - i)&0xFF);
		}
		assertEquals(0x113fdb5c, Crc32C.value(buf, 0, buf.length));
		
		byte[] data = new byte[]{
			    (byte)0x01,  (byte)0xc0,  (byte)0x00,  (byte)0x00,
			    (byte)0x00,  (byte)0x00,  (byte)0x00,  (byte)0x00,
			    (byte)0x00,  (byte)0x00,  (byte)0x00,  (byte)0x00,
			    (byte)0x00,  (byte)0x00,  (byte)0x00,  (byte)0x00,
			    (byte)0x14,  (byte)0x00,  (byte)0x00,  (byte)0x00,
			    (byte)0x00,  (byte)0x00,  (byte)0x04,  (byte)0x00,
			    (byte)0x00,  (byte)0x00,  (byte)0x00,  (byte)0x14,
			    (byte)0x00,  (byte)0x00,  (byte)0x00,  (byte)0x18,
			    (byte)0x28,  (byte)0x00,  (byte)0x00,  (byte)0x00,
			    (byte)0x00,  (byte)0x00,  (byte)0x00,  (byte)0x00,
			    (byte)0x02,  (byte)0x00,  (byte)0x00,  (byte)0x00,
			    (byte)0x00,  (byte)0x00,  (byte)0x00,  (byte)0x00,
		};
		assertEquals(0xd9963a56L, Crc32C.value(data, 0, data.length));
	}
	
	@Test
	public void testValues() {
		byte[] b1 = "a".getBytes();
		byte[] b2 = "foo".getBytes();
		assertTrue(Crc32C.value(b1, 0, b1.length) != Crc32C.value(b2, 0, b2.length));
	}
	
	@Test
	public void testExtend() {
		byte[] b1 = "hello world".getBytes();
		byte[] b2 = "hello ".getBytes();
		byte[] b3 = "world".getBytes();
		assertEquals(Crc32C.value(b1, 0, b1.length),
				Crc32C.extend(Crc32C.value(b2,0,b2.length), b3, 0, b3.length));
	}
	
	@Test
	public void testMask() {
		byte[] b1 = "foo".getBytes();
		long crc = Crc32C.value(b1, 0, b1.length);
		assertTrue(crc != Crc32C.mask(crc));
		assertTrue(crc != Crc32C.mask(Crc32C.mask(crc)));
		assertEquals(crc, Crc32C.unmask(Crc32C.mask(crc)));
		assertEquals(crc, Crc32C.unmask(Crc32C.unmask(Crc32C.mask(Crc32C.mask(crc)))));
	}
}
