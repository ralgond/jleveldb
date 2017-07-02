package com.tchaicatkovsky.jleveldb.test;

import org.junit.Test;

import com.tchaicatkovsky.jleveldb.util.ByteBuf;
import com.tchaicatkovsky.jleveldb.util.ByteBufFactory;
import com.tchaicatkovsky.jleveldb.util.DefaultByteBuf;
import com.tchaicatkovsky.jleveldb.util.DefaultSlice;
import com.tchaicatkovsky.jleveldb.util.ReflectionUtil;
import com.tchaicatkovsky.jleveldb.util.Slice;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestDefaultByteBuf {
	@Test
	public void testRequire() throws Exception {
		DefaultByteBuf buf = (DefaultByteBuf)ByteBufFactory.defaultByteBuf(); 
		
		assertTrue(buf.capacity() == 0);
		
		buf.require(1);
		assertTrue(buf.capacity() == 16);
		
		buf.require(2);
		assertTrue(buf.capacity() == 16);
		
		buf.require(16);
		assertTrue(buf.capacity() == 16);
		
		buf.require(17);
		assertTrue(buf.capacity() == 32);
		
		ReflectionUtil.setValue(buf, "limit", buf.offset()+16);
		buf.require(17);
		assertTrue(buf.capacity() == 64);
	}
	
	@Test
	public void testAppend() {
		int ARRAY_SIZE = 127;
		
		byte[] a = new byte[ARRAY_SIZE];
		for (int i = 0; i < ARRAY_SIZE; i++)
			a[i] = (byte)i;
		
		for (int COPY_LEN = 1; COPY_LEN <= 127; COPY_LEN++) {
			DefaultByteBuf buf1 = (DefaultByteBuf)ByteBufFactory.defaultByteBuf();
			int offset = 0;
			int len = COPY_LEN;
			while (offset < a.length) {
				len = ((a.length - offset < len) ? a.length - offset : len);
				buf1.append(a, offset, len);
				offset += len;
			}
			
			assertTrue(buf1.capacity() == 128);
			assertTrue(buf1.data()[ARRAY_SIZE] == 0);
			
			for (int i = 0; i < a.length; i++) {
				assertTrue(buf1.data()[i] == a[i]);
			}
		}
	}
	
	@Test
	public void testFixedCoding01() throws Exception {
		DefaultByteBuf buf = (DefaultByteBuf)ByteBufFactory.defaultByteBuf();
		buf.writeFixedNat32(0);
		buf.writeFixedNat32(Short.MAX_VALUE);
		buf.writeFixedNat32(Integer.MAX_VALUE);
		
		assertTrue(buf.readFixedNat32() == 0);
		assertTrue(buf.readFixedNat32() == Short.MAX_VALUE);
		assertTrue(buf.readFixedNat32() == Integer.MAX_VALUE);
		
		buf.clear();
		
		buf.writeFixedNat64(0);
		buf.writeFixedNat64(Short.MAX_VALUE);
		buf.writeFixedNat64(Integer.MAX_VALUE);
		buf.writeFixedNat64(Long.MAX_VALUE);

		assertTrue(buf.readFixedNat64() == 0);
		assertTrue(buf.readFixedNat64() == Short.MAX_VALUE);
		assertTrue(buf.readFixedNat64() == Integer.MAX_VALUE);
		assertTrue(buf.readFixedNat64() == Long.MAX_VALUE);
		
		buf.clear();
		
		buf.writeFixedNat64(Long.MAX_VALUE);
		buf.writeFixedNat32(0);
		buf.writeFixedNat64(Integer.MAX_VALUE);
		buf.writeFixedNat32(Short.MAX_VALUE);
		buf.writeFixedNat64(Short.MAX_VALUE);
		buf.writeFixedNat32(Integer.MAX_VALUE);
		buf.writeFixedNat64(0);
		
		assertTrue(buf.readFixedNat64() == Long.MAX_VALUE);
		assertTrue(buf.readFixedNat32() == 0);
		assertTrue(buf.readFixedNat64() == Integer.MAX_VALUE);
		assertTrue(buf.readFixedNat32() == Short.MAX_VALUE);
		assertTrue(buf.readFixedNat64() == Short.MAX_VALUE);
		assertTrue(buf.readFixedNat32() == Integer.MAX_VALUE);
		assertTrue(buf.readFixedNat64() == 0);
	}
	
	@Test
	public void testVarCoding01() throws Exception {
		DefaultByteBuf buf = (DefaultByteBuf)ByteBufFactory.defaultByteBuf();
		buf.writeVarNat32(0);
		buf.writeVarNat32(Short.MAX_VALUE);
		buf.writeVarNat32(Integer.MAX_VALUE);
		
		assertTrue(buf.readVarNat32() == 0);
		assertTrue(buf.readVarNat32() == Short.MAX_VALUE);
		assertTrue(buf.readVarNat32() == Integer.MAX_VALUE);
		
		buf.clear();
		
		buf.writeVarNat64(0);
		buf.writeVarNat64(Short.MAX_VALUE);
		buf.writeVarNat64(Integer.MAX_VALUE);
		buf.writeVarNat64(Long.MAX_VALUE);

		assertTrue(buf.readVarNat64() == 0);
		assertTrue(buf.readVarNat64() == Short.MAX_VALUE);
		assertTrue(buf.readVarNat64() == Integer.MAX_VALUE);
		assertTrue(buf.readVarNat64() == Long.MAX_VALUE);
		
		buf.clear();
		
		buf.writeVarNat64(Long.MAX_VALUE);
		buf.writeVarNat32(0);
		buf.writeVarNat64(Integer.MAX_VALUE);
		buf.writeVarNat32(Short.MAX_VALUE);
		buf.writeVarNat64(Short.MAX_VALUE);
		buf.writeVarNat32(Integer.MAX_VALUE);
		buf.writeVarNat64(0);
		
		assertTrue(buf.readVarNat64() == Long.MAX_VALUE);
		assertTrue(buf.readVarNat32() == 0);
		assertTrue(buf.readVarNat64() == Integer.MAX_VALUE);
		assertTrue(buf.readVarNat32() == Short.MAX_VALUE);
		assertTrue(buf.readVarNat64() == Short.MAX_VALUE);
		assertTrue(buf.readVarNat32() == Integer.MAX_VALUE);
		assertTrue(buf.readVarNat64() == 0);
	}
	
	@Test
	public void testResize() {
		ByteBuf buf = ByteBufFactory.defaultByteBuf();
		Slice s = new DefaultSlice("123456");
		buf.append(s.data(), s.offset(), s.size());
		
		buf.resize(3);
		//System.out.println(buf.encodeToString());
		assertEquals("123", buf.encodeToString());
		
		buf.resize(5, (byte)0x30);
		//System.out.println(buf.encodeToString());
		assertEquals("12300", buf.encodeToString());
	}
}
