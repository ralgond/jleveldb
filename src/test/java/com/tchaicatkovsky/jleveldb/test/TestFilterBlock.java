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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Test;

import com.tchaicatkovsky.jleveldb.FilterPolicy;
import com.tchaicatkovsky.jleveldb.table.FilterBlockBuilder;
import com.tchaicatkovsky.jleveldb.table.FilterBlockReader;
import com.tchaicatkovsky.jleveldb.util.ByteBuf;
import com.tchaicatkovsky.jleveldb.util.Coding;
import com.tchaicatkovsky.jleveldb.util.Hash;
import com.tchaicatkovsky.jleveldb.util.Slice;
import com.tchaicatkovsky.jleveldb.util.SliceFactory;

public class TestFilterBlock {
	// For testing: emit an array with one hash value per key
	static class TestHashFilter extends FilterPolicy {
		@Override
		public String name() {
		    return "TestHashFilter";
		}
		
		@Override
		public void createFilter(List<Slice> keys, ByteBuf dst) {
		    for (int i = 0; i < keys.size(); i++) {
		    	Slice s = keys.get(i);	    	
		    	long h = Hash.hash0(s.data(), s.offset(), s.size(), 1L);
		      	dst.addFixedNat32Long(h);
		    }
		}
		
		public boolean keyMayMatch(Slice key, Slice filter) {
		    long h = Hash.hash0(key.data(), key.offset(), key.size(), 1L);
		    for (int i = 0; i + 4 <= filter.size(); i += 4) {
		    	long hash = Coding.decodeFixedNat32Long(filter.data(), filter.offset() + i);
		    	if (h == hash) {
		    		return true;
		    	}
		    }
		    return false;
		}
		
		@Override
		public void delete() {
			
		}
	}
	
	@Test
	public void testEmptyBuilder() {
		TestHashFilter policy = new TestHashFilter();
		FilterBlockBuilder builder = new FilterBlockBuilder(policy);
		Slice block = builder.finish();
		assertEquals("\\x00\\x00\\x00\\x00\\x0b", block.escapeString());
		FilterBlockReader reader = new FilterBlockReader(policy, block);
		assertTrue(reader.keyMayMatch(0, SliceFactory.newUnpooled("foo")));
		assertTrue(reader.keyMayMatch(100000, SliceFactory.newUnpooled("foo")));
	}
	
	@Test
	public void testSingleChunk() {
		TestHashFilter policy = new TestHashFilter();
		FilterBlockBuilder builder = new FilterBlockBuilder(policy);
		builder.startBlock(100);
		builder.addKey(SliceFactory.newUnpooled("foo"));
		builder.addKey(SliceFactory.newUnpooled("bar"));
		builder.addKey(SliceFactory.newUnpooled("box"));
		builder.startBlock(200);
		builder.addKey(SliceFactory.newUnpooled("box"));
		builder.startBlock(300);
		builder.addKey(SliceFactory.newUnpooled("hello"));
		Slice block = builder.finish();
		FilterBlockReader reader = new FilterBlockReader(policy, block);
		assertTrue(reader.keyMayMatch(100, SliceFactory.newUnpooled("foo")));
		assertTrue(reader.keyMayMatch(100, SliceFactory.newUnpooled("bar")));
		assertTrue(reader.keyMayMatch(100, SliceFactory.newUnpooled("box")));
		assertTrue(reader.keyMayMatch(100, SliceFactory.newUnpooled("hello")));
		assertTrue(reader.keyMayMatch(100, SliceFactory.newUnpooled("foo")));
		assertFalse(reader.keyMayMatch(100, SliceFactory.newUnpooled("missing")));
		assertFalse(reader.keyMayMatch(100, SliceFactory.newUnpooled("other")));
	}
	
	@Test
	public void testMultiChunk() {
		TestHashFilter policy = new TestHashFilter();
		FilterBlockBuilder builder = new FilterBlockBuilder(policy);

		  // First filter
		  builder.startBlock(0);
		  builder.addKey(SliceFactory.newUnpooled("foo"));
		  builder.startBlock(2000);
		  builder.addKey(SliceFactory.newUnpooled("bar"));

		  // Second filter
		  builder.startBlock(3100);
		  builder.addKey(SliceFactory.newUnpooled("box"));

		  // Third filter is empty

		  // Last filter
		  builder.startBlock(9000);
		  builder.addKey(SliceFactory.newUnpooled("box"));
		  builder.addKey(SliceFactory.newUnpooled("hello"));

		  Slice block = builder.finish();
		  FilterBlockReader reader = new FilterBlockReader(policy, block);

		  // Check first filter
		  assertTrue(reader.keyMayMatch(0, SliceFactory.newUnpooled("foo")));
		  assertTrue(reader.keyMayMatch(2000, SliceFactory.newUnpooled("bar")));
		  assertTrue(! reader.keyMayMatch(0, SliceFactory.newUnpooled("box")));
		  assertTrue(! reader.keyMayMatch(0, SliceFactory.newUnpooled("hello")));

		  // Check second filter
		  assertTrue(reader.keyMayMatch(3100, SliceFactory.newUnpooled("box")));
		  assertTrue(! reader.keyMayMatch(3100, SliceFactory.newUnpooled("foo")));
		  assertTrue(! reader.keyMayMatch(3100, SliceFactory.newUnpooled("bar")));
		  assertTrue(! reader.keyMayMatch(3100, SliceFactory.newUnpooled("hello")));

		  // Check third filter (empty)
		  assertTrue(! reader.keyMayMatch(4100, SliceFactory.newUnpooled("foo")));
		  assertTrue(! reader.keyMayMatch(4100, SliceFactory.newUnpooled("bar")));
		  assertTrue(! reader.keyMayMatch(4100, SliceFactory.newUnpooled("box")));
		  assertTrue(! reader.keyMayMatch(4100, SliceFactory.newUnpooled("hello")));

		  // Check last filter
		  assertTrue(reader.keyMayMatch(9000, SliceFactory.newUnpooled("box")));
		  assertTrue(reader.keyMayMatch(9000, SliceFactory.newUnpooled("hello")));
		  assertTrue(! reader.keyMayMatch(9000, SliceFactory.newUnpooled("foo")));
		  assertTrue(! reader.keyMayMatch(9000, SliceFactory.newUnpooled("bar")));
	}
}
