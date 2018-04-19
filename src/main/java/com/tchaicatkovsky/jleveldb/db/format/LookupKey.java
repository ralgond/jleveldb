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
import com.tchaicatkovsky.jleveldb.util.ByteBufFactory;
import com.tchaicatkovsky.jleveldb.util.Slice;
import com.tchaicatkovsky.jleveldb.util.SliceFactory;

/**
 * A helper class useful for DBImpl.get()
 */
public class LookupKey {
	
	// We construct a char array of the form:
	//    klength  varint32               <-- start_
	//    userkey  char[klength]          <-- kstart_
	//    tag      uint64
	//                                    <-- end_
	// The array is a suitable MemTable key.
	// The suffix starting with "userkey" can be used as an InternalKey.	
	byte[] data;
    int start;
    int kstart;
    int end;
    long sequence;
    
	public LookupKey(Slice userKey, long sequence) {
		ByteBuf buf = ByteBufFactory.newUnpooled();
		buf.require(1);
		
		start = 0;
		int usize = userKey.size();
		
		buf.addVarNat32(usize + 8);
		
		kstart = buf.endOffset();
		
		buf.append(userKey.data(), usize);
		buf.addFixedNat64(DBFormat.packSequenceAndType(sequence, DBFormat.kValueTypeForSeek));
		
		end = buf.endOffset();
		
		data = buf.data();
		
		this.sequence = sequence;
	}
	
	/**
	 * Return an internal key (suitable for passing to an internal iterator)
	 * @return
	 */
	public Slice internalKey() { 
		return SliceFactory.newUnpooled(data, kstart, end - kstart); 
	}
	
	/**
	 *  Return the user key
	 * @return
	 */
	public Slice userKey() { 
		return SliceFactory.newUnpooled(data, kstart, end - kstart - 8); 
	}
	
	
}
