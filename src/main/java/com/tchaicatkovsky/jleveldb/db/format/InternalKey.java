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

import com.tchaicatkovsky.jleveldb.util.ByteBuf;
import com.tchaicatkovsky.jleveldb.util.ByteBufFactory;
import com.tchaicatkovsky.jleveldb.util.Slice;
import com.tchaicatkovsky.jleveldb.util.SliceFactory;

/**
 * Modules in this directory should keep internal keys wrapped inside
 * the following class instead of plain strings so that we do not
 * incorrectly use string comparisons instead of an InternalKeyComparator.
 */
public class InternalKey {
	ByteBuf rep;
	
	public InternalKey() {
		rep = ByteBufFactory.newUnpooled();
	}
	
	public ByteBuf rep() {
		return rep;
	}
	
	protected static void appendInternalKey(ByteBuf result, ParsedInternalKey key) {
		result.append(key.userKey.data(), key.userKey.size());
		result.addFixedNat64(DBFormat.packSequenceAndType(key.sequence, key.type));
	}
	
	public InternalKey(Slice userKey, long s, ValueType t) {
		this();
		appendInternalKey(rep, new ParsedInternalKey(userKey, s, t));
	}
	
	public void decodeFrom(Slice s) {
		rep.assign(s.data(), s.offset(), s.size());
	}
	
	public void decodeFrom(ByteBuf b) {
		rep.assign(b.data(), b.size());
	}
	
	public Slice encode() {
		return SliceFactory.newUnpooled(rep);
	}
	
	public Slice userKey() {
		return DBFormat.extractUserKey(SliceFactory.newUnpooled(rep));
	}
	
	public void setFrom(ParsedInternalKey p) {
		rep.clear();
		DBFormat.appendInternalKey(rep, p);
	}
	
	public void clear() {
		rep.clear();
	}
	
	@Override
	public String toString() {
		return null;
	}
	
	public void assgin(InternalKey ik) {
		rep.assign(ik.rep);
	}
	
	public String debugString() {
		String result;
		ParsedInternalKey parsed = new ParsedInternalKey();
		if (parsed.parse(SliceFactory.newUnpooled(rep))) {
			result = parsed.debugString();
		} else {
		    result = "(bad)";
		    result += rep.escapeString();
		}
		return result;
	}
	
	@Override
	public InternalKey clone() {
		InternalKey ik = new InternalKey();
		ik.rep.assign(rep);
		return ik;
	}
}
