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
 * This file is translated from source code file Copyright (c) 2011 
 * The LevelDB Authors and licensed under the BSD-3-Clause license.
 */

package com.tchaicatkovsky.jleveldb.db.format;

import com.tchaicatkovsky.jleveldb.util.ByteBuf;
import com.tchaicatkovsky.jleveldb.util.Coding;
import com.tchaicatkovsky.jleveldb.util.Slice;
import com.tchaicatkovsky.jleveldb.util.SliceFactory;

public class DBFormat {

	public static final int kNumLevels = 7;
	
	/**
	 * Level-0 compaction is started when we hit this many files.
	 */
	public static final int kL0_CompactionTrigger = 4;
	
	/** 
	 * Soft limit on number of level-0 files.  We slow down writes at this point.
	 */
	public static final int kL0_SlowdownWritesTrigger = 8;
	
	/**
	 *  Maximum number of level-0 files.  We stop writes at this point.
	 */
	public static final int kL0_StopWritesTrigger = 12;

	/**
	 * Maximum level to which a new compacted memtable is pushed if it
	 * does not create overlap.  We try to push to level 2 to avoid the
	 * relatively expensive level 0=>1 compactions and to avoid some
	 * expensive manifest file operations.  We do not push all the way to
	 * the largest level since that can generate a lot of wasted disk
	 * space if the same key space is being repeatedly overwritten.
	 */
	public static final int kMaxMemCompactLevel = 2;
	
	/**
	 *  Approximate gap in bytes between samples of data read during iteration.
	 */
	public static final int kReadBytesPeriod = 1048576;
	
	/**
	 * kValueTypeForSeek defines the ValueType that should be passed when 
	 * constructing a ParsedInternalKey object for seeking to a particular
	 * sequence number (since we sort sequence numbers in decreasing order
	 * and the value type is embedded as the low 8 bits in the sequence 
	 * number in internal keys, we need to use the highest-numbered 
	 * ValueType, not the lowest).
	 */
	public static final ValueType kValueTypeForSeek = ValueType.Value;
	
	public static final long kMaxSequenceNumber = Long.MAX_VALUE >> 8;
	

	/**
	 *  Returns the user key portion of an internal key.
	 * @param internalKey
	 * @return
	 */
	final public static Slice extractUserKey(Slice internalKey) {
		assert(internalKey.size() >= 8);
		return SliceFactory.newUnpooled(internalKey.data(), internalKey.offset(), internalKey.size() - 8);
	}
	
	final public static ValueType extractValueType(Slice internalKey) {
		assert(internalKey.size() >= 8);
		int n = internalKey.size();
		long num = Coding.decodeFixedNat64(internalKey.data(), internalKey.offset() + n - 8);
		byte c = (byte)(num & 0xffL);
		return ValueType.create(c);
	}
	
	final public static long packSequenceAndType(long seq, ValueType t) {
		assert(seq <= kMaxSequenceNumber);
		assert(t.type() <= kValueTypeForSeek.type());
		return ((seq << 8) | (t.type() & 0x0ffL));
	}

	public static void appendInternalKey(ByteBuf result, ParsedInternalKey key) {
		result.append(key.userKey.data(), key.userKey.size());
		result.addFixedNat64(packSequenceAndType(key.sequence, key.type));
	}
}
