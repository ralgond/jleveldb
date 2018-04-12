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

package com.tchaicatkovsky.jleveldb.db;

import com.tchaicatkovsky.jleveldb.Status;
import com.tchaicatkovsky.jleveldb.WritableFile;
import com.tchaicatkovsky.jleveldb.db.LogFormat.RecordType;
import com.tchaicatkovsky.jleveldb.util.Coding;
import com.tchaicatkovsky.jleveldb.util.Crc32C;
import com.tchaicatkovsky.jleveldb.util.Slice;
import com.tchaicatkovsky.jleveldb.util.SliceFactory;

public class LogWriter {
	
	WritableFile dest;
	
	/**
	 * Current offset in block
	 */
	int blockOffset;

	/**
	 * crc32c values for all supported record types.  These are
	 * pre-computed to reduce the overhead of computing the crc of the 
	 * record type stored in the header.
	 */
	long typeCrc[] = new long[LogFormat.kMaxRecordType + 1];
	  
	static void initTypeCrc(long[] typeCrc) {
		byte[] tmp = new byte[1];
		for (int i = 0; i <= LogFormat.kMaxRecordType; i++) {
			tmp[0] = (byte)i;
			typeCrc[i] = Crc32C.value(tmp, 0, 1);
		}
	}
	
	/**
	 * Create a writer that will append data to "dest".</br>
	 * "dest" must be initially empty.</br>
	 * "dest" must remain live while this Writer is in use.</br>
	 * 
	 * @param dest
	 */
	public LogWriter(WritableFile dest) {
		this.dest = dest;
		blockOffset = 0;
		initTypeCrc(typeCrc);
	}
	
	/**
	 * Create a writer that will append data to "dest".</br>
	 * "dest" must have initial length "dest_length".</br>
	 * "dest" must remain live while this Writer is in use.</br>
	 * 
	 * @param dest
	 * @param destLength
	 */
	public LogWriter(WritableFile dest, long destLength) {
		this.dest = dest;
		blockOffset = (int)(destLength % LogFormat.kBlockSize);
		initTypeCrc(typeCrc);
	}
	
	public static final byte[] fillzero = new byte[]{0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0};
	
	public Status addRecord(Slice slice) {
		byte[] ptr = slice.data();
		int ptrOffset = slice.offset();
		int left = slice.size();

		// Fragment the record if necessary and emit it.  Note that if slice
		// is empty, we still want to iterate once to emit a single
		// zero-length record
		Status s = Status.ok0();
		boolean begin = true;
		do {
			int leftover = LogFormat.kBlockSize - blockOffset;
		    assert(leftover >= 0);
		    if (leftover < LogFormat.kHeaderSize) {
		    	// Switch to a new block
		    	if (leftover > 0) {
		    		// Fill the trailer (literal below relies on kHeaderSize being 7)
		    		dest.append(SliceFactory.newUnpooled(fillzero, 0, leftover));
		    	}
		    	blockOffset = 0;
		    }

		    // Invariant: we never leave < kHeaderSize bytes in a block.
		    assert(LogFormat.kBlockSize - blockOffset - LogFormat.kHeaderSize >= 0);

		    int avail = LogFormat.kBlockSize - blockOffset - LogFormat.kHeaderSize;
		    int fragmentLength = (left < avail) ? left : avail;

		    RecordType type;
		    boolean end = (left == fragmentLength);
		    if (begin && end) {
		    	type = LogFormat.RecordType.FullType;
		    } else if (begin) {
		    	type = LogFormat.RecordType.FirstType;
		    } else if (end) {
		    	type = LogFormat.RecordType.LastType;
		    } else {
		    	type = LogFormat.RecordType.MiddleType;
		    }

		    s = emitPhysicalRecord(type, ptr, ptrOffset, fragmentLength);
		    ptrOffset += fragmentLength;
		    left -= fragmentLength;
		    begin = false;
		} while (s.ok() && left > 0);
		return s;
	}
	
	Status emitPhysicalRecord(RecordType t, byte[] ptr, int offset, int n) {
		assert(n <= 0xffff);  // Must fit in two bytes
		assert(blockOffset + LogFormat.kHeaderSize + n <= LogFormat.kBlockSize);

		// Format the header
		byte buf[] = new byte[LogFormat.kHeaderSize];
		buf[4] = (byte)(n & 0xff);
		buf[5] = (byte)((n >> 8) & 0xff);
		buf[6] = (byte)(t.getType() & 0xff);

		// Compute the crc of the record type and the payload.
		long crc = Crc32C.extend(typeCrc[t.getType()], ptr, offset, n);
		crc = Crc32C.mask(crc);                 // Adjust for storage
		Coding.encodeFixedNat32Long(buf, 0, crc);

		// Write the header and the payload
		Status s = dest.append(SliceFactory.newUnpooled(buf, 0, LogFormat.kHeaderSize));
		if (s.ok()) {
			s = dest.append(SliceFactory.newUnpooled(ptr, offset, n));
			if (s.ok()) {
				s = dest.flush();
			}
		}
		blockOffset += (LogFormat.kHeaderSize + n);
		return s;
	}
	
	public void delete() {
		
	}
}
