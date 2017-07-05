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
