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

public class BytewiseComparatorImpl extends Comparator0 {
	private static BytewiseComparatorImpl INSTANCE = new BytewiseComparatorImpl();
	
	public static BytewiseComparatorImpl getInstance() {
		return INSTANCE;
	}
	
	@Override
	public int compare(byte[] a, int aoff, int asize, byte[] b, int boff, int bsize) {
		int r = 0;
		for (int i = 0; i < asize && i < bsize; i++) {
			if (a[aoff+i] == b[boff+i])
				continue;
			r = ((a[aoff+i]&0xff) < (b[boff+i]&0xff)) ? -1 : +1;
			break;
		}
		
		if (r == 0) {
			 if (asize < bsize) 
				 r = -1;
			 else if (asize > bsize) 
				 r = +1;
		}
		
		return r;
	}

	@Override
	public String name() {
		return "leveldb.BytewiseComparator";
	}

	@Override
	public void findShortestSeparator(ByteBuf start, Slice limit) {
	    // Find length of common prefix for start and limit.
	    int minLength = Integer.min(start.size(), limit.size());
	    int diffIndex = 0;
	    while ((diffIndex < minLength) &&
	           (start.getByte(diffIndex) == limit.getByte(diffIndex))) {
	    	diffIndex++;
	    }

	    if (diffIndex >= minLength) {
	        // Do not shorten if one string is a prefix of the other
	    } else {
	        int diffByte = (start.getByte(diffIndex) & 0xff);
	        if (diffByte < 0xff && diffByte + 1 < (limit.getByte(diffIndex) & 0xff)) {
	        	start.setByte(diffIndex, (byte)(diffByte + 1));
	        	start.resize(diffIndex + 1);
	        	assert(compare(start, limit) < 0);
	        }
	    }
	}

	@Override
	public void findShortSuccessor(ByteBuf key) {
	    // Find first character that can be incremented.
	    int n = key.size();
	    for (int i = 0; i < n; i++) {
	    	int b = (key.getByte(i) & 0x0ff);
	    	if (b != 0xff) {
	    		key.setByte(i, (byte)(b + 1));
	    		key.resize(i+1);
	    		return;
	    	}
	    }
	    // key is a run of 0xffs.  Leave it alone.
	}
}
