package com.tchaicatkovsky.jleveldb.test;


import org.junit.Test;

import com.tchaicatkovsky.jleveldb.util.ByteBuf;
import com.tchaicatkovsky.jleveldb.util.ByteBufFactory;
import com.tchaicatkovsky.jleveldb.util.DefaultSlice;
import com.tchaicatkovsky.jleveldb.util.Slice;

//import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestSlice {
	@Test
	public void testEquals() {
		byte[] data = new byte[]{1,2,3};
		Slice s1 = new DefaultSlice(data, 0, data.length);
		Slice s2 = new DefaultSlice(data, 0, data.length);
		
		assertTrue(s1.equals(s2));
		
		byte[] data2 = new byte[]{1,2,3,1,2,3};
		Slice s3 = new DefaultSlice(data2, 3, 3);
		assertTrue(s1.equals(s3));
		
		Slice s4 = new DefaultSlice(data2, 0, data2.length);
		assertTrue(!s1.equals(s4));
	}
	
	@Test
	public void testWithByteBuf() {
		byte[] data = new byte[]{1,2,3};
		Slice s1 = new DefaultSlice(data, 0, data.length);
		
		ByteBuf buf = ByteBufFactory.defaultByteBuf();
		buf.addByte((byte)1);
		buf.addByte((byte)2);
		buf.addByte((byte)3);
		
		assertTrue(s1.equals(new DefaultSlice(buf)));
	}
	
	@Test
	public void testWithString() {
		Slice s1 = new DefaultSlice("abc123");
		s1.removePrefix(3);
		assertTrue(s1.encodeToString().equals("123"));
	}
	
	@Test
	public void testCompare() {
		Slice s1 = new DefaultSlice("123");
		Slice s2 = new DefaultSlice("234");
		Slice s3 = new DefaultSlice("123");
		Slice s4 = new DefaultSlice("1");
		
		assertTrue(s1.compare(s2) < 0);
		assertTrue(s1.compare(s3) == 0);
		assertTrue(s2.compare(s1) > 0);
		
		assertTrue(s4.compare(s1) < 0);
	}
}
