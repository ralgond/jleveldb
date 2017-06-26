package org.ht.jleveldb.test;

import org.ht.jleveldb.db.format.InternalKeyComparator;
import org.ht.jleveldb.util.ByteBuf;
import org.ht.jleveldb.util.ByteBufFactory;
import org.ht.jleveldb.util.BytewiseComparatorImpl;
import org.ht.jleveldb.util.Slice;
import org.junit.Test;

public class TestComparator0 {
	@Test
	public void testBytewiseComparatorImpl() {
		Slice a = new Slice("abc");
		Slice b = new Slice("9999");
		int ret = BytewiseComparatorImpl.getInstance().compare(a, b);
		System.out.println(ret);
	}
	
	@Test
	public void testBytewiseComparatorImpl1() {
		byte[] abuf = new byte[]{(byte)0x0a, (byte)0xfd, (byte)0xfd, (byte)0x01, (byte)0x03, 
								 (byte)0x00, (byte)0x00, (byte)0x00, (byte)0x00, (byte)0x00};
		ByteBuf a = ByteBufFactory.defaultByteBuf(abuf, abuf.length);
		

		byte[] bbuf = new byte[]{(byte)0x09, (byte)0xfd, (byte)0x01, (byte)0x02,
								 (byte)0x00, (byte)0x00, (byte)0x00, (byte)0x00, (byte)0x00};
		ByteBuf b = ByteBufFactory.defaultByteBuf(bbuf, bbuf.length);
		
		InternalKeyComparator icmp = new InternalKeyComparator(BytewiseComparatorImpl.getInstance());
		//int ret = BytewiseComparatorImpl.getInstance().compare(a, b);
		int ret = icmp.compare(a, b);
		System.out.println(ret);
	}
}
