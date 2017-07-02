package com.tchaicatkovsky.jleveldb.test;


import org.junit.Test;

import com.tchaicatkovsky.jleveldb.db.format.InternalKeyComparator;
import com.tchaicatkovsky.jleveldb.db.format.ValueType;
import com.tchaicatkovsky.jleveldb.util.ByteBuf;
import com.tchaicatkovsky.jleveldb.util.ByteBufFactory;
import com.tchaicatkovsky.jleveldb.util.BytewiseComparatorImpl;
import com.tchaicatkovsky.jleveldb.util.DefaultSlice;
import com.tchaicatkovsky.jleveldb.util.Slice;

import static org.junit.Assert.assertEquals;

public class TestInternalKeyComparator {
	@Test
	public void test01() {
		InternalKeyComparator ikcmp = new InternalKeyComparator(BytewiseComparatorImpl.getInstance());
		int ret = 0;
		
		Slice auk = new DefaultSlice("abc");
		ByteBuf abuf = ByteBufFactory.defaultByteBuf(); 
		abuf.append(auk.data(), auk.offset(), auk.size());
		abuf.writeFixedNat64(InternalKeyComparator.packSequenceAndType(1, ValueType.Value));


		Slice buk = new DefaultSlice("abc");
		ByteBuf bbuf = ByteBufFactory.defaultByteBuf(); 
		bbuf.append(buk.data(), buk.offset(), buk.size());
		bbuf.writeFixedNat64(InternalKeyComparator.packSequenceAndType(1, ValueType.Value));
	
		ret = ikcmp.compare(new DefaultSlice(abuf), new DefaultSlice(bbuf));
		//System.out.println(ret);
		assertEquals(ret, 1);
		
		auk = new DefaultSlice("123");
		abuf = ByteBufFactory.defaultByteBuf(); 
		abuf.append(auk.data(), auk.offset(), auk.size());
		abuf.writeFixedNat64(InternalKeyComparator.packSequenceAndType(1, ValueType.Value));


		buk = new DefaultSlice("234");
		bbuf = ByteBufFactory.defaultByteBuf(); 
		bbuf.append(buk.data(), buk.offset(), buk.size());
		bbuf.writeFixedNat64(InternalKeyComparator.packSequenceAndType(1, ValueType.Value));
		
		ret = ikcmp.compare(new DefaultSlice(abuf), new DefaultSlice(bbuf));
		//System.out.println(ret);
		assertEquals(ret, -1);
		

		buk = new DefaultSlice("12");
		bbuf = ByteBufFactory.defaultByteBuf(); 
		bbuf.append(buk.data(), buk.offset(), buk.size());
		bbuf.writeFixedNat64(InternalKeyComparator.packSequenceAndType(1, ValueType.Value));
		
		ret = ikcmp.compare(new DefaultSlice(abuf), new DefaultSlice(bbuf));
		//System.out.println(ret);
		assertEquals(ret, 1);
		
		buk = new DefaultSlice("123");
		bbuf = ByteBufFactory.defaultByteBuf(); 
		bbuf.append(buk.data(), buk.offset(), buk.size());
		bbuf.writeFixedNat64(InternalKeyComparator.packSequenceAndType(2, ValueType.Value));
		
		ret = ikcmp.compare(new DefaultSlice(abuf), new DefaultSlice(bbuf));
		//System.out.println(ret);
		assertEquals(ret, 1);
		

		buk = new DefaultSlice("123");
		bbuf = ByteBufFactory.defaultByteBuf(); 
		bbuf.append(buk.data(), buk.offset(), buk.size());
		bbuf.writeFixedNat64(InternalKeyComparator.packSequenceAndType(1, ValueType.Deletion));
		
		ret = ikcmp.compare(new DefaultSlice(abuf), new DefaultSlice(bbuf));
		//System.out.println(ret);
		assertEquals(ret, -1);
	}
}
