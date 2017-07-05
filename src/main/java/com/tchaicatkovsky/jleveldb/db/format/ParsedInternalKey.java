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
package com.tchaicatkovsky.jleveldb.db.format;

import com.tchaicatkovsky.jleveldb.util.Coding;
import com.tchaicatkovsky.jleveldb.util.Slice;
import com.tchaicatkovsky.jleveldb.util.SliceFactory;
import com.tchaicatkovsky.jleveldb.util.Strings;

public class ParsedInternalKey {
	public Slice userKey;
	public long sequence;
	public ValueType type;

	public ParsedInternalKey() {
		// Intentionally left uninitialized (for speed)
	}  
	
	public ParsedInternalKey(Slice u, long seq, ValueType t) {
		userKey = u;
		sequence = seq;
		type = t;
	}
	
	/** 
	 * Return the length of the encoding of "key".
	 * @return
	 */
	final public int encodingLength() {
		return userKey.size() + 8;
	}
	
	private final static long kSequenceNumberMask = ~(0x0FFL << (64 - 8));
	
	/**
	 * Attempt to parse an internal key from "internalKey".  On success,
	 * stores the parsed data in "result", and returns true.</br>
	 * 
	 * On error, returns false, leaves "result" in an undefined state.
	 * @param internalKey
	 * @return
	 */
	public boolean parse(Slice internalKey) {
		int n = internalKey.size();
		if (n < 8) 
			return false;
		
		long num = Coding.decodeFixedNat64(internalKey.data(), internalKey.offset()+n-8);
	    byte c = (byte)(num & 0xffL);
	    sequence = ((num >> 8) & kSequenceNumberMask);
		if (c == ValueType.Deletion.type()) {
			type = ValueType.Deletion;
		} else if (c == ValueType.Value.type()) {
			type = ValueType.Value;
		} else {
			return false;
		}
	    userKey = SliceFactory.newUnpooled(internalKey.data(), internalKey.offset(), n - 8);
	    
	    return true;
	}
	
	public String debugString() {
		return Strings.escapeString(userKey) + String.format("' @ %d : %d", sequence, type.type);
	}
};
