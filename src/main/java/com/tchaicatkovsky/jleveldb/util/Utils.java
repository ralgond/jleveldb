/**
 * Copyright (c) 2017-2018 Teng Huang <ht201509 at 163 dot com>
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
 * 
 * Some portions of this file are translated from source code file Copyright (c) 2011 
 * The LevelDB Authors and licensed under the BSD-3-Clause license.
 */

package com.tchaicatkovsky.jleveldb.util;

import java.util.Map;

public class Utils {
	public static Slice randomString(Random0 rnd, int len, ByteBuf dst) {
		dst.resize(len);
		for (int i = 0; i < len; i++) {
			dst.setByte(i, (byte)(0x20+rnd.uniform(95)));  // ' ' .. '~'
		}
		return SliceFactory.newUnpooled(dst.data(), dst.offset(), dst.size());
	}

	public static ByteBuf randomKey(Random0 rnd, int len) {
		// Make sure to generate a wide variety of characters so we
		// test the boundary conditions for short-key optimizations.
		byte[] kTestChars = new byte[] {
		    //'\0', '\1', 'a', 'b', 'c', 'd', 'e', '\xfd', '\xfe', '\xff'
			(byte)0x00, (byte)0x01, (byte)0x61, (byte)0x62, (byte)0x63, (byte)0x64, (byte)0x65, 
			(byte)0xfd, (byte)0xfe, (byte)0xff
		};
		
		ByteBuf result = ByteBufFactory.newUnpooled() ;
		for (int i = 0; i < len; i++) {
			result.addByte(kTestChars[(int)rnd.uniform(kTestChars.length)]);;
		}
		return result;
	}


	public static Slice compressibleString(Random0 rnd, double compressed_fraction,
		                                int len, ByteBuf dst) {
		int raw = (int)(len * compressed_fraction);
		if (raw < 1) 
			raw = 1;
		ByteBuf raw_data = ByteBufFactory.newUnpooled();
		randomString(rnd, raw, raw_data);

		// Duplicate the random data until we have filled "len" bytes
		dst.clear();
		while (dst.size() < len) {
			dst.append(raw_data);
		}
		dst.resize(len);
		return SliceFactory.newUnpooled(dst.data(), dst.offset(), dst.size());
	}
	
	public static String tmpDir() {
		return "./data/test";
	}
	
	public static int randomSeed() {
		Map<String, String> map = System.getenv();
		String seed = map.get("TEST_RANDOM_SEED");
		return (seed == null) ? 301 : Integer.parseInt(seed);
	}
	
	public static String makeString(int n, char c) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < n; i++)
			sb.append(c);
		return sb.toString();
	}
}
