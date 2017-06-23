package org.ht.jleveldb.test;


import org.junit.Test;

import static org.junit.Assert.assertEquals;

import org.ht.jleveldb.db.format.InternalKeyComparator;
import org.ht.jleveldb.db.format.ValueType;
import org.ht.jleveldb.util.ByteBuf;
import org.ht.jleveldb.util.ByteBufFactory;
import org.ht.jleveldb.util.BytewiseComparatorImpl;
import org.ht.jleveldb.util.Slice;

public class TestInternalKeyComparator {
	@Test
	public void test01() {
		InternalKeyComparator ikcmp = new InternalKeyComparator(BytewiseComparatorImpl.getInstance());
		int ret = 0;
		
		Slice auk = new Slice("abc");
		ByteBuf abuf = ByteBufFactory.defaultByteBuf(); 
		abuf.append(auk.data, auk.offset, auk.size());
		abuf.writeFixedNat64(InternalKeyComparator.packSequenceAndType(1, ValueType.Value));


		Slice buk = new Slice("abc");
		ByteBuf bbuf = ByteBufFactory.defaultByteBuf(); 
		bbuf.append(buk.data, buk.offset, buk.size());
		bbuf.writeFixedNat64(InternalKeyComparator.packSequenceAndType(1, ValueType.Value));
	
		ret = ikcmp.compare(new Slice(abuf), new Slice(bbuf));
		//System.out.println(ret);
		assertEquals(ret, 1);
		
		auk = new Slice("123");
		abuf = ByteBufFactory.defaultByteBuf(); 
		abuf.append(auk.data, auk.offset, auk.size());
		abuf.writeFixedNat64(InternalKeyComparator.packSequenceAndType(1, ValueType.Value));


		buk = new Slice("234");
		bbuf = ByteBufFactory.defaultByteBuf(); 
		bbuf.append(buk.data, buk.offset, buk.size());
		bbuf.writeFixedNat64(InternalKeyComparator.packSequenceAndType(1, ValueType.Value));
		
		ret = ikcmp.compare(new Slice(abuf), new Slice(bbuf));
		//System.out.println(ret);
		assertEquals(ret, -1);
		

		buk = new Slice("12");
		bbuf = ByteBufFactory.defaultByteBuf(); 
		bbuf.append(buk.data, buk.offset, buk.size());
		bbuf.writeFixedNat64(InternalKeyComparator.packSequenceAndType(1, ValueType.Value));
		
		ret = ikcmp.compare(new Slice(abuf), new Slice(bbuf));
		//System.out.println(ret);
		assertEquals(ret, 1);
		
		buk = new Slice("123");
		bbuf = ByteBufFactory.defaultByteBuf(); 
		bbuf.append(buk.data, buk.offset, buk.size());
		bbuf.writeFixedNat64(InternalKeyComparator.packSequenceAndType(2, ValueType.Value));
		
		ret = ikcmp.compare(new Slice(abuf), new Slice(bbuf));
		//System.out.println(ret);
		assertEquals(ret, 1);
		

		buk = new Slice("123");
		bbuf = ByteBufFactory.defaultByteBuf(); 
		bbuf.append(buk.data, buk.offset, buk.size());
		bbuf.writeFixedNat64(InternalKeyComparator.packSequenceAndType(1, ValueType.Deletion));
		
		ret = ikcmp.compare(new Slice(abuf), new Slice(bbuf));
		//System.out.println(ret);
		assertEquals(ret, -1);
	}
}
