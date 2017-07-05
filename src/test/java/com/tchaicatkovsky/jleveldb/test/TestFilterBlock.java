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
import com.tchaicatkovsky.jleveldb.util.UnpooledSlice;
import com.tchaicatkovsky.jleveldb.util.Hash;
import com.tchaicatkovsky.jleveldb.util.Slice;
import com.tchaicatkovsky.jleveldb.util.Strings;

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
//		    	System.out.printf("createFilter, [%d], s={offset=%d,size=%d}, s0=%s, h=%d\n", 
//		    			i, s.offset, s.size(), s.encodeToString(), h);
		      	dst.writeFixedNat32Long(h);
		    }
		}
		
		public boolean keyMayMatch(Slice key, Slice filter) {
		    long h = Hash.hash0(key.data(), key.offset(), key.size(), 1L);
//		    System.out.println("keyMayMatch, target="+h);
		    for (int i = 0; i + 4 <= filter.size(); i += 4) {
		    	long hash = Coding.decodeFixedNat32Long(filter.data(), filter.offset() + i);
//		    	System.out.printf("keyMayMatch, [%d] hash=%d\n", i, hash);
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
//		System.out.println("==="+Strings.escapeString(block)+"===");
		assertEquals("\\x00\\x00\\x00\\x00\\x0b", Strings.escapeString(block));
		FilterBlockReader reader = new FilterBlockReader(policy, block);
		assertTrue(reader.keyMayMatch(0, new UnpooledSlice("foo")));
		assertTrue(reader.keyMayMatch(100000, new UnpooledSlice("foo")));
	}
	
	@Test
	public void testSingleChunk() {
		TestHashFilter policy = new TestHashFilter();
		FilterBlockBuilder builder = new FilterBlockBuilder(policy);
		builder.startBlock(100);
		builder.addKey(new UnpooledSlice("foo"));
		builder.addKey(new UnpooledSlice("bar"));
		builder.addKey(new UnpooledSlice("box"));
		builder.startBlock(200);
		builder.addKey(new UnpooledSlice("box"));
		builder.startBlock(300);
		builder.addKey(new UnpooledSlice("hello"));
		Slice block = builder.finish();
		FilterBlockReader reader = new FilterBlockReader(policy, block);
		assertTrue(reader.keyMayMatch(100, new UnpooledSlice("foo")));
		assertTrue(reader.keyMayMatch(100, new UnpooledSlice("bar")));
		assertTrue(reader.keyMayMatch(100, new UnpooledSlice("box")));
		assertTrue(reader.keyMayMatch(100, new UnpooledSlice("hello")));
		assertTrue(reader.keyMayMatch(100, new UnpooledSlice("foo")));
		assertFalse(reader.keyMayMatch(100, new UnpooledSlice("missing")));
		assertFalse(reader.keyMayMatch(100, new UnpooledSlice("other")));
	}
	
	@Test
	public void testMultiChunk() {
		TestHashFilter policy = new TestHashFilter();
		FilterBlockBuilder builder = new FilterBlockBuilder(policy);

		  // First filter
		  builder.startBlock(0);
		  builder.addKey(new UnpooledSlice("foo"));
		  builder.startBlock(2000);
		  builder.addKey(new UnpooledSlice("bar"));

		  // Second filter
		  builder.startBlock(3100);
		  builder.addKey(new UnpooledSlice("box"));

		  // Third filter is empty

		  // Last filter
		  builder.startBlock(9000);
		  builder.addKey(new UnpooledSlice("box"));
		  builder.addKey(new UnpooledSlice("hello"));

		  Slice block = builder.finish();
		  FilterBlockReader reader = new FilterBlockReader(policy, block);

		  // Check first filter
		  assertTrue(reader.keyMayMatch(0, new UnpooledSlice("foo")));
		  assertTrue(reader.keyMayMatch(2000, new UnpooledSlice("bar")));
		  assertTrue(! reader.keyMayMatch(0, new UnpooledSlice("box")));
		  assertTrue(! reader.keyMayMatch(0, new UnpooledSlice("hello")));

		  // Check second filter
		  assertTrue(reader.keyMayMatch(3100, new UnpooledSlice("box")));
		  assertTrue(! reader.keyMayMatch(3100, new UnpooledSlice("foo")));
		  assertTrue(! reader.keyMayMatch(3100, new UnpooledSlice("bar")));
		  assertTrue(! reader.keyMayMatch(3100, new UnpooledSlice("hello")));

		  // Check third filter (empty)
		  assertTrue(! reader.keyMayMatch(4100, new UnpooledSlice("foo")));
		  assertTrue(! reader.keyMayMatch(4100, new UnpooledSlice("bar")));
		  assertTrue(! reader.keyMayMatch(4100, new UnpooledSlice("box")));
		  assertTrue(! reader.keyMayMatch(4100, new UnpooledSlice("hello")));

		  // Check last filter
		  assertTrue(reader.keyMayMatch(9000, new UnpooledSlice("box")));
		  assertTrue(reader.keyMayMatch(9000, new UnpooledSlice("hello")));
		  assertTrue(! reader.keyMayMatch(9000, new UnpooledSlice("foo")));
		  assertTrue(! reader.keyMayMatch(9000, new UnpooledSlice("bar")));
	}
}
