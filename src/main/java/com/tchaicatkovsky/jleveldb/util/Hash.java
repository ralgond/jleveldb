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
 * 
 * This file is translated from source code file Copyright (c) 2011 
 * The LevelDB Authors and licensed under the BSD-3-Clause license.
 */

package com.tchaicatkovsky.jleveldb.util;

public class Hash {
	final static long kUint32Mask = 0xFFFFFFFFL;

	/**
	 * Similar to murmur hash
	 * @param data
	 * @param offset
	 * @param n
	 * @param seed uint32
	 * @return
	 */
	public static long hash0(byte[] data, int offset, int n, long seed) {
		long m = 0xc6a4a793L;
		long r = 24L;
		int limit = offset + n;
		long h = ((seed ^ (n * m)) & kUint32Mask);
		
		// Pick up four bytes at a time
		while (offset + 4 <= limit) {
		    long w = (Coding.decodeFixedNat32Long(data, offset) & kUint32Mask);
		    offset += 4;
		    h += w; h &= kUint32Mask;
		    h *= m; h &= kUint32Mask;
		    h ^= (h >> 16); h &= kUint32Mask;
		}
		
		// Pick up remaining bytes
		switch (limit - offset) {
		case 3:
			h += ((data[offset+2] & 0x0ffL) << 16); h &= kUint32Mask;
		case 2:
			h += ((data[offset+1] & 0x0ffL) << 8); h &= kUint32Mask;
		case 1:
			h += (data[offset] & 0x0ffL);
			h *= m; h &= kUint32Mask;
			h ^= (h >> r); h &= kUint32Mask;
			break;
		}
		
		return h & kUint32Mask;
	}
}
