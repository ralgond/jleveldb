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

package com.tchaicatkovsky.jleveldb.db;

import com.tchaicatkovsky.jleveldb.Status;
import com.tchaicatkovsky.jleveldb.WriteBatch;
import com.tchaicatkovsky.jleveldb.db.format.ValueType;
import com.tchaicatkovsky.jleveldb.util.Coding;
import com.tchaicatkovsky.jleveldb.util.Slice;
import com.tchaicatkovsky.jleveldb.util.SliceFactory;

//WriteBatchInternal provides static methods for manipulating a
//WriteBatch that we don't want in the public WriteBatch interface.
public class WriteBatchInternal {
	
	/**
	 * WriteBatch header has an 8-byte sequence number followed by a 4-byte count.
	 */
	public static int kHEADER = 12;
	
    /** 
     * Return the number of entries in the batch.
     * @param b
     * @return
     */
    public static int count(WriteBatch b) {
    	return Coding.decodeFixedNat32(b.rep.data(), 8);
    }

    /** 
     * Set the count for the number of entries in the batch.
     * @param b
     * @param n
     */
    public static void setCount(WriteBatch b, int n) {
    	Coding.encodeFixedNat32(b.rep.data(), 8, 8 + 4, n);
    }

    /** 
     * Return the sequence number for the start of this batch.
     * @param b
     * @return
     */
    public static long sequence(WriteBatch b) {
    	return  Coding.decodeFixedNat64(b.rep.data(), 0);
    }

    /**
     *  Store the specified number as the sequence number for the start of this batch.
     * @param b
     * @param seq
     */
    public static void setSequence(WriteBatch b, long seq) {
    	Coding.encodeFixedNat64(b.rep.data(), 0, 8, seq);
    }

    public static Slice contents(WriteBatch batch) {
        return SliceFactory.newUnpooled(batch.rep);
    }

    public static long byteSize(WriteBatch batch) {
        return batch.rep.size();
    }
    
    static class MemTableInserter implements WriteBatch.Handler {
    	long sequence;
    	MemTable memtable;

    	public MemTableInserter(long sequence, MemTable memtable) {
    		this.sequence = sequence;
    		this.memtable = memtable;
    	}
    	
    	public void put(Slice key, Slice value) {
    		memtable.add(sequence, ValueType.Value, key, value);
    	    sequence++;
    	}
    	
    	public void delete(Slice key) {
    		memtable.add(sequence, ValueType.Deletion, key, SliceFactory.newUnpooled());
    		sequence++;
    	}
    };

    public static Status insertInto(WriteBatch b, MemTable memtable) {
    	return b.iterate(new MemTableInserter(WriteBatchInternal.sequence(b), memtable));
    }
    
    public static void setContents(WriteBatch b, Slice contents) {
    	assert(contents.size() >= kHEADER);
    	b.rep.assign(contents.data(), contents.offset(), contents.size());
    }

    public static void append(WriteBatch dst, WriteBatch src) {
    	setCount(dst, count(dst) + count(src));
    	assert(src.rep.size() >= kHEADER);
    	dst.rep.append(src.rep.data(), kHEADER, src.rep.size() - kHEADER);
    }
}
