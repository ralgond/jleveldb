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

import java.util.Comparator;
import java.util.concurrent.atomic.AtomicLong;

import com.tchaicatkovsky.jleveldb.Iterator0;
import com.tchaicatkovsky.jleveldb.Status;
import com.tchaicatkovsky.jleveldb.db.format.DBFormat;
import com.tchaicatkovsky.jleveldb.db.format.InternalKeyComparator;
import com.tchaicatkovsky.jleveldb.db.format.LookupKey;
import com.tchaicatkovsky.jleveldb.db.format.ValueType;
import com.tchaicatkovsky.jleveldb.util.ByteBuf;
import com.tchaicatkovsky.jleveldb.util.Coding;
import com.tchaicatkovsky.jleveldb.util.Object0;
import com.tchaicatkovsky.jleveldb.util.ReferenceCounted;
import com.tchaicatkovsky.jleveldb.util.Slice;
import com.tchaicatkovsky.jleveldb.util.SliceFactory;

public class MemTable implements ReferenceCounted {
	
	static class KeyValueSlice {
		public byte[] data;
		public int keyOffset;
		public int internalKeySize;
		public int valueOffset;
		public int valueSize;
		
		public KeyValueSlice(byte[] data, int keyOffset, int internalKeySize, int valueOffset, int valueSize) {
			this.data = data;
			this.keyOffset = keyOffset;
			this.internalKeySize = internalKeySize;
			this.valueOffset = valueOffset;
			this.valueSize = valueSize;
		}
		
		final public Slice internalKeySlice() {
			return SliceFactory.newUnpooled(data, keyOffset, internalKeySize);
		}
		
		final public Slice value() {
			return SliceFactory.newUnpooled(data, valueOffset, valueSize);
		}
	}
	
	static class TableKeyComparator implements Comparator<Slice>{
		InternalKeyComparator comparator;
		
		public TableKeyComparator(InternalKeyComparator comparator) {
			this.comparator = comparator;
		}
		
	    public int compare(Slice a, Slice b) {
	    	return comparator.compare(a, b);
	    }
	}
	
	static class MemTableIterator extends Iterator0 {
		
		SkipListMap<Slice, KeyValueSlice>.Iterator1 iter;
		
		public MemTableIterator(SkipListMap<Slice, KeyValueSlice> table) {
			iter = table.iterator1();
		}
		
		public void delete() {
			super.delete();
			iter = null;
		}
		
		public boolean valid() {
			return iter.valid();
		}
		
		public void seekToFirst() {
			iter.seekToFirst();
		}
		
		public void seekToLast() {
			iter.seekToLast();
		}
		
		public void seek(Slice target0) {
			iter.seek(target0);
		}
		
		public void next() {
			iter.next();
		}
		
		public void prev() {
			iter.prev();
		}
		
		public Slice key() {
			return iter.key();
		}
		
		public Slice value() {
			return iter.value().value();
		}
		
		public Status status() {
			return Status.ok0();
		}
	}
	
	TableKeyComparator comparator;
	int refs;
	SkipListMap<Slice,KeyValueSlice> table;
	AtomicLong approximateMemory = new AtomicLong(0);
	MemTableArena arena;
	
	public MemTable(InternalKeyComparator c) {
		comparator = new TableKeyComparator(c);
		refs = 0;
		table = new SkipListMap<Slice,KeyValueSlice>(12, 4, comparator);
		arena = new MemTableArena();
	}
	
	public void delete() {
		assert(refs == 0);
		table = null;
		arena.delete();
	}
	
	/**
	 *  Increase reference count.
	 */
	public void ref() { 
		++refs; 
	}

	/** 
	 * Drop reference count.  Delete if no more references exist.
	 */
	public void unref() {
		--refs;
		assert(refs >= 0);
	    if (refs == 0) {
	    	delete();
	    }
	}

	/**
	 * Returns an estimate of the number of bytes of data in use by this data structure. </br>
	 * It is safe to call when MemTable is being modified.</br>
	 */
	public long approximateMemoryUsage() {
		return arena.memoryUsage();
	}
	
	public int entrySize() {
		return table.size();
	}
	
	/**
	 * Return an iterator that yields the contents of the memtable.</br></br>
	 * 
	 * The caller must ensure that the underlying MemTable remains live
	 * while the returned iterator is live.  The keys returned by this
	 * iterator are internal keys encoded by AppendInternalKey in the
	 * db/format.{h,cc} module.</br></br>
	 */
	public Iterator0 newIterator() {
		return new MemTableIterator(table);
	}
	
	/**
	 * Add an entry into memtable that maps key to value at the
	 * specified sequence number and with the specified type.</br>
	 * Typically value will be empty if type==kTypeDeletion.</br></br>
	 * 
	 * @param seq
	 * @param type ValueType
	 * @param key Slice
	 * @param value Slice
	 */
	public void add(long seq, ValueType type, Slice key, Slice value) {
		// Format:
		//   output : {internalKeySize:varint32, key:byte[internalKeySize], valSize:varint32, value:byte[valSize]}
		//   internalKey : {data:byte[size], (seq&type):uint64}
		
		int keyOffset = -1;
		int userKeySize = key.size();
		int valueOffset = -1;
		int valueSize = value.size();
		int internalKeySize = userKeySize + 8;
		int encodedLen = Coding.varNatLength(internalKeySize) + internalKeySize + Coding.varNatLength(valueSize) + valueSize;
				
		Slice s = arena.allocate(encodedLen);
		byte[] data = s.data();
		int initialOffset = s.offset();
		Coding.appendVarNat32(s, internalKeySize);
		keyOffset = s.offset();
		if (key.size() > 0)
			System.arraycopy(key.data(), key.offset(), data, keyOffset, key.size());
		s.incrOffset(key.size());
		Coding.appendFixNat64(s, (seq << 8) | (type.type() & 0xFFL));
		Coding.appendVarNat32(s, valueSize);
		valueOffset = s.offset();
		if (value.size() > 0)
			System.arraycopy(value.data(), value.offset(), data, valueOffset, value.size());
		s.incrOffset(value.size());
		assert(s.offset() - initialOffset == encodedLen);
		
		
		KeyValueSlice kvs = new KeyValueSlice(data, keyOffset, internalKeySize, valueOffset, valueSize);
		Slice keySlice = SliceFactory.newUnpooled(data, keyOffset, internalKeySize);
	
		table.put(keySlice, kvs);
	}
	
	/**
	 * If memtable contains a value for key, store it in *value and return true.</br>
	 * If memtable contains a deletion for key, store a NotFound() error in *status and return true.</br>
	 * Else, return false.</br></br>
	 * 
	 * @param key
	 * @param value
	 * @param s
	 * @return
	 */
	public boolean get(LookupKey key, ByteBuf value, Object0<Status> s) {
		if (value != null)
			value.clear();
		Slice memkey = key.internalKey();
		SkipListMap<Slice,KeyValueSlice>.Iterator1 iter = table.iterator1();
		iter.seek(memkey);
		if (iter.valid()) {
			Slice ikey = iter.key();
		    if (comparator.comparator.userComparator().compare(
		    		SliceFactory.newUnpooled(ikey.data(), ikey.offset(), ikey.size()-8), 
		    		key.userKey()) == 0) {
		    	// Correct user key
		    	ValueType vtype = DBFormat.extractValueType(ikey);
		    	if (vtype != null) {
			    	switch (vtype) {
			        case Value: {
			        	Slice v = iter.value().value();
			        	value.assign(v.data(), v.offset(), v.size());
			        	return true;
			        }
			        case Deletion:
			        	s.setValue(Status.notFound(null));
			        	return true;
			    	}
		    	}
		    }
		}
		return false;
	}
	
	
	/**
	 * Encode a suitable internal key target for "target" and return it.</br>
	 * Uses scratch as scratch space.</br>
	 * 
	 * @param scratch [OUTPUT]
	 * @param target
	 */
	static void EncodeKey(ByteBuf scratch, Slice target) {
		scratch.clear();
		scratch.addVarNat32(target.size());
		scratch.append(target.data(), target.size());
	}
	
	
	static Slice getLengthPrefixedSlice(byte[] data, int offset) {
		Slice tmp = SliceFactory.newUnpooled(data, offset, 5);
		int len = Coding.popVarNat32(tmp);  // +5: we assume "p" is not corrupted
		offset = tmp.offset();
		tmp.init(data, offset, len);
		return tmp;
	}
}
