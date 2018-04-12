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

package com.tchaicatkovsky.jleveldb.test;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.tchaicatkovsky.jleveldb.Iterator0;
import com.tchaicatkovsky.jleveldb.Status;
import com.tchaicatkovsky.jleveldb.WriteBatch;
import com.tchaicatkovsky.jleveldb.db.MemTable;
import com.tchaicatkovsky.jleveldb.db.WriteBatchInternal;
import com.tchaicatkovsky.jleveldb.db.format.InternalKeyComparator;
import com.tchaicatkovsky.jleveldb.db.format.ParsedInternalKey;
import com.tchaicatkovsky.jleveldb.util.BytewiseComparatorImpl;
import com.tchaicatkovsky.jleveldb.util.Slice;
import com.tchaicatkovsky.jleveldb.util.SliceFactory;

public class TestWriteBatch {

	static String printContents(WriteBatch b) {
		InternalKeyComparator cmp = new InternalKeyComparator(BytewiseComparatorImpl.getInstance());
		MemTable mem = new MemTable(cmp);
		mem.ref();
		String state = "";
		Status s = WriteBatchInternal.insertInto(b, mem);
		int count = 0;
		Iterator0 iter = mem.newIterator();
		for (iter.seekToFirst(); iter.valid(); iter.next()) {
			Slice ikey = iter.key();
			ParsedInternalKey pikey = new ParsedInternalKey();
			pikey.parse(ikey);
			switch (pikey.type) {
			case Value:
				state += "Put(";
				// state += ikey.encodeToString();
				state += pikey.userKey.encodeToString();
				state += ", ";
				state += iter.value().encodeToString();
				state += ")";
				count++;
				break;
			case Deletion:
				state += "Delete(";
				state += pikey.userKey.encodeToString();
				// state += ikey.encodeToString();
				state += ")";
				count++;
				break;
			}
			state += "@";
			state += pikey.sequence;
		}
		iter.delete();
		if (!s.ok()) {
			state += "ParseError()";
		} else if (count != WriteBatchInternal.count(b)) {
			state += "CountMismatch()";
		}
		mem.unref();
		return state;
	}

	@Test
	public void testEmpty() {
		WriteBatch batch = new WriteBatch();
		assertEquals("", printContents(batch));
		assertEquals(0, WriteBatchInternal.count(batch));
	}

	@Test
	public void testMultiple() {
		WriteBatch batch = new WriteBatch();
		batch.put(SliceFactory.newUnpooled("foo"), SliceFactory.newUnpooled("bar"));
		batch.delete(SliceFactory.newUnpooled("box"));
		batch.put(SliceFactory.newUnpooled("baz"), SliceFactory.newUnpooled("boo"));
		WriteBatchInternal.setSequence(batch, 100);
		assertEquals(100, WriteBatchInternal.sequence(batch));
		assertEquals(3, WriteBatchInternal.count(batch));

		assertEquals("Put(baz, boo)@102" + "Delete(box)@101" + "Put(foo, bar)@100", printContents(batch));
	}

	@Test
	public void testCorruption() {
		WriteBatch batch = new WriteBatch();
		batch.put(SliceFactory.newUnpooled("foo"), SliceFactory.newUnpooled("bar"));
		batch.delete(SliceFactory.newUnpooled("box"));
		WriteBatchInternal.setSequence(batch, 200);
		Slice contents = WriteBatchInternal.contents(batch);
		WriteBatchInternal.setContents(batch, SliceFactory.newUnpooled(contents.data(), contents.offset(), contents.size() - 1));
		assertEquals("Put(foo, bar)@200ParseError()", printContents(batch));
	}

	@Test
	public void testAppend() {
		WriteBatch b1 = new WriteBatch();
		WriteBatch b2 = new WriteBatch();

		WriteBatchInternal.setSequence(b1, 200);
		WriteBatchInternal.setSequence(b2, 300);
		WriteBatchInternal.append(b1, b2);
		assertEquals("", printContents(b1));
		b2.put(SliceFactory.newUnpooled("a"), SliceFactory.newUnpooled("va"));
		WriteBatchInternal.append(b1, b2);
		assertEquals("Put(a, va)@200", printContents(b1));
		b2.clear();
		b2.put(SliceFactory.newUnpooled("b"), SliceFactory.newUnpooled("vb"));
		WriteBatchInternal.append(b1, b2);
		assertEquals("Put(a, va)@200Put(b, vb)@201", printContents(b1));
		b2.delete(SliceFactory.newUnpooled("foo"));
		WriteBatchInternal.append(b1, b2);
		assertEquals("Put(a, va)@200" + "Put(b, vb)@202" + "Put(b, vb)@201" + "Delete(foo)@203", printContents(b1));
	}
}
