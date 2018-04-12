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

import org.junit.Test;

import com.tchaicatkovsky.jleveldb.Status;
import com.tchaicatkovsky.jleveldb.db.VersionEdit;
import com.tchaicatkovsky.jleveldb.db.format.InternalKey;
import com.tchaicatkovsky.jleveldb.db.format.ValueType;
import com.tchaicatkovsky.jleveldb.util.ByteBuf;
import com.tchaicatkovsky.jleveldb.util.ByteBufFactory;
import com.tchaicatkovsky.jleveldb.util.SliceFactory;

import static org.junit.Assert.assertTrue;

public class TestVersionEdit {
	static void testEncodeDecode(VersionEdit edit) {
		  ByteBuf encoded = ByteBufFactory.newUnpooled();
		  ByteBuf encoded2 =  ByteBufFactory.newUnpooled();
		  edit.encodeTo(encoded);
		  VersionEdit parsed = new VersionEdit();
		  Status s = parsed.decodeFrom(SliceFactory.newUnpooled(encoded));
		  assertTrue(s.ok());
		  parsed.encodeTo(encoded2);
		  assertTrue(encoded.equals(encoded2));
	}
	
	@Test
	public void testEncodeDecode() {
		long kBig = 1L << 50;

		VersionEdit edit = new VersionEdit();
		for (int i = 0; i < 4; i++) {
		    testEncodeDecode(edit);
		    edit.addFile(3, kBig + 300 + i, kBig + 400 + i,
		                 new InternalKey(SliceFactory.newUnpooled("foo"), kBig + 500 + i, ValueType.Value),
		                 new InternalKey(SliceFactory.newUnpooled("zoo"), kBig + 600 + i, ValueType.Deletion),
		                 10);
		    edit.deleteFile(4, kBig + 700 + i);
		    edit.setCompactPointer(i, new InternalKey(SliceFactory.newUnpooled("x"), kBig + 900 + i, ValueType.Value));
		}

		edit.setComparatorName("foo");
		edit.setLogNumber(kBig + 100);
		edit.setNextFile(kBig + 200);
		edit.setLastSequence(kBig + 1000);
		testEncodeDecode(edit);
	}
}
