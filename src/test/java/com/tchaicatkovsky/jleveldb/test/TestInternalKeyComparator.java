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

import com.tchaicatkovsky.jleveldb.db.format.InternalKeyComparator;
import com.tchaicatkovsky.jleveldb.db.format.ValueType;
import com.tchaicatkovsky.jleveldb.util.ByteBuf;
import com.tchaicatkovsky.jleveldb.util.ByteBufFactory;
import com.tchaicatkovsky.jleveldb.util.BytewiseComparatorImpl;
import com.tchaicatkovsky.jleveldb.util.Slice;
import com.tchaicatkovsky.jleveldb.util.SliceFactory;

import static org.junit.Assert.assertEquals;

public class TestInternalKeyComparator {
	@Test
	public void test01() {
		InternalKeyComparator ikcmp = new InternalKeyComparator(BytewiseComparatorImpl.getInstance());
		int ret = 0;
		
		Slice auk = SliceFactory.newUnpooled("abc");
		ByteBuf abuf = ByteBufFactory.newUnpooled(); 
		abuf.append(auk.data(), auk.offset(), auk.size());
		abuf.addFixedNat64(InternalKeyComparator.packSequenceAndType(1, ValueType.Value));


		Slice buk = SliceFactory.newUnpooled("abc");
		ByteBuf bbuf = ByteBufFactory.newUnpooled(); 
		bbuf.append(buk.data(), buk.offset(), buk.size());
		bbuf.addFixedNat64(InternalKeyComparator.packSequenceAndType(1, ValueType.Value));
	
		ret = ikcmp.compare(SliceFactory.newUnpooled(abuf), SliceFactory.newUnpooled(bbuf));

		assertEquals(ret, 1);
		
		auk = SliceFactory.newUnpooled("123");
		abuf = ByteBufFactory.newUnpooled(); 
		abuf.append(auk.data(), auk.offset(), auk.size());
		abuf.addFixedNat64(InternalKeyComparator.packSequenceAndType(1, ValueType.Value));


		buk = SliceFactory.newUnpooled("234");
		bbuf = ByteBufFactory.newUnpooled(); 
		bbuf.append(buk.data(), buk.offset(), buk.size());
		bbuf.addFixedNat64(InternalKeyComparator.packSequenceAndType(1, ValueType.Value));
		
		ret = ikcmp.compare(SliceFactory.newUnpooled(abuf), SliceFactory.newUnpooled(bbuf));

		assertEquals(ret, -1);
		

		buk = SliceFactory.newUnpooled("12");
		bbuf = ByteBufFactory.newUnpooled(); 
		bbuf.append(buk.data(), buk.offset(), buk.size());
		bbuf.addFixedNat64(InternalKeyComparator.packSequenceAndType(1, ValueType.Value));
		
		ret = ikcmp.compare(SliceFactory.newUnpooled(abuf), SliceFactory.newUnpooled(bbuf));

		assertEquals(ret, 1);
		
		buk = SliceFactory.newUnpooled("123");
		bbuf = ByteBufFactory.newUnpooled(); 
		bbuf.append(buk.data(), buk.offset(), buk.size());
		bbuf.addFixedNat64(InternalKeyComparator.packSequenceAndType(2, ValueType.Value));
		
		ret = ikcmp.compare(SliceFactory.newUnpooled(abuf), SliceFactory.newUnpooled(bbuf));

		assertEquals(ret, 1);
		

		buk = SliceFactory.newUnpooled("123");
		bbuf = ByteBufFactory.newUnpooled(); 
		bbuf.append(buk.data(), buk.offset(), buk.size());
		bbuf.addFixedNat64(InternalKeyComparator.packSequenceAndType(1, ValueType.Deletion));
		
		ret = ikcmp.compare(SliceFactory.newUnpooled(abuf), SliceFactory.newUnpooled(bbuf));

		assertEquals(ret, -1);
	}
}
