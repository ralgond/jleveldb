package com.tchaicatkovsky.jleveldb.test;


import org.junit.Test;

import com.tchaicatkovsky.jleveldb.db.format.InternalKeyComparator;
import com.tchaicatkovsky.jleveldb.db.format.ValueType;
import com.tchaicatkovsky.jleveldb.util.ByteBuf;
import com.tchaicatkovsky.jleveldb.util.ByteBufFactory;
import com.tchaicatkovsky.jleveldb.util.BytewiseComparatorImpl;
import com.tchaicatkovsky.jleveldb.util.UnpooledSlice;
import com.tchaicatkovsky.jleveldb.util.Slice;

import static org.junit.Assert.assertEquals;

public class TestInternalKeyComparator {
	@Test
	public void test01() {
		InternalKeyComparator ikcmp = new InternalKeyComparator(BytewiseComparatorImpl.getInstance());
		int ret = 0;
		
		Slice auk = new UnpooledSlice("abc");
		ByteBuf abuf = ByteBufFactory.newUnpooled(); 
		abuf.append(auk.data(), auk.offset(), auk.size());
		abuf.writeFixedNat64(InternalKeyComparator.packSequenceAndType(1, ValueType.Value));


		Slice buk = new UnpooledSlice("abc");
		ByteBuf bbuf = ByteBufFactory.newUnpooled(); 
		bbuf.append(buk.data(), buk.offset(), buk.size());
		bbuf.writeFixedNat64(InternalKeyComparator.packSequenceAndType(1, ValueType.Value));
	
		ret = ikcmp.compare(new UnpooledSlice(abuf), new UnpooledSlice(bbuf));
		//System.out.println(ret);
		assertEquals(ret, 1);
		
		auk = new UnpooledSlice("123");
		abuf = ByteBufFactory.newUnpooled(); 
		abuf.append(auk.data(), auk.offset(), auk.size());
		abuf.writeFixedNat64(InternalKeyComparator.packSequenceAndType(1, ValueType.Value));


		buk = new UnpooledSlice("234");
		bbuf = ByteBufFactory.newUnpooled(); 
		bbuf.append(buk.data(), buk.offset(), buk.size());
		bbuf.writeFixedNat64(InternalKeyComparator.packSequenceAndType(1, ValueType.Value));
		
		ret = ikcmp.compare(new UnpooledSlice(abuf), new UnpooledSlice(bbuf));
		//System.out.println(ret);
		assertEquals(ret, -1);
		

		buk = new UnpooledSlice("12");
		bbuf = ByteBufFactory.newUnpooled(); 
		bbuf.append(buk.data(), buk.offset(), buk.size());
		bbuf.writeFixedNat64(InternalKeyComparator.packSequenceAndType(1, ValueType.Value));
		
		ret = ikcmp.compare(new UnpooledSlice(abuf), new UnpooledSlice(bbuf));
		//System.out.println(ret);
		assertEquals(ret, 1);
		
		buk = new UnpooledSlice("123");
		bbuf = ByteBufFactory.newUnpooled(); 
		bbuf.append(buk.data(), buk.offset(), buk.size());
		bbuf.writeFixedNat64(InternalKeyComparator.packSequenceAndType(2, ValueType.Value));
		
		ret = ikcmp.compare(new UnpooledSlice(abuf), new UnpooledSlice(bbuf));
		//System.out.println(ret);
		assertEquals(ret, 1);
		

		buk = new UnpooledSlice("123");
		bbuf = ByteBufFactory.newUnpooled(); 
		bbuf.append(buk.data(), buk.offset(), buk.size());
		bbuf.writeFixedNat64(InternalKeyComparator.packSequenceAndType(1, ValueType.Deletion));
		
		ret = ikcmp.compare(new UnpooledSlice(abuf), new UnpooledSlice(bbuf));
		//System.out.println(ret);
		assertEquals(ret, -1);
	}
}
