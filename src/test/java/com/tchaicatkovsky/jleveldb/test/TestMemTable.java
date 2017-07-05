package com.tchaicatkovsky.jleveldb.test;


import org.junit.Test;

import com.tchaicatkovsky.jleveldb.Iterator0;
import com.tchaicatkovsky.jleveldb.Status;
import com.tchaicatkovsky.jleveldb.db.MemTable;
import com.tchaicatkovsky.jleveldb.db.format.DBFormat;
import com.tchaicatkovsky.jleveldb.db.format.InternalKeyComparator;
import com.tchaicatkovsky.jleveldb.db.format.LookupKey;
import com.tchaicatkovsky.jleveldb.db.format.ValueType;
import com.tchaicatkovsky.jleveldb.util.ByteBuf;
import com.tchaicatkovsky.jleveldb.util.ByteBufFactory;
import com.tchaicatkovsky.jleveldb.util.BytewiseComparatorImpl;
import com.tchaicatkovsky.jleveldb.util.UnpooledSlice;
import com.tchaicatkovsky.jleveldb.util.Object0;
import com.tchaicatkovsky.jleveldb.util.Slice;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestMemTable {
	@Test
	public void test01() {
		InternalKeyComparator ikcmp = new InternalKeyComparator(BytewiseComparatorImpl.getInstance());
		
		MemTable memtable = new MemTable(ikcmp);
		memtable.add(1, ValueType.Value, new UnpooledSlice("1"), new UnpooledSlice("1"));
		memtable.add(1, ValueType.Value, new UnpooledSlice("2"), new UnpooledSlice("2"));
		memtable.add(1, ValueType.Value, new UnpooledSlice("3"), new UnpooledSlice("3"));
		memtable.add(1, ValueType.Value, new UnpooledSlice("4"), new UnpooledSlice("4"));
		memtable.add(1, ValueType.Value, new UnpooledSlice("5"), new UnpooledSlice("5"));
		
		String[] ary = new String[]{"1","2","3","4","5"};
		int idx = 0;
		Iterator0 it = memtable.newIterator();
		it.seekToFirst();
		while (it.valid()) {
			Slice ikey = it.key();
			//System.out.println(k.encodeToString());
			assertEquals(DBFormat.extractUserKey(ikey).encodeToString(), ary[idx++]);
			it.next();
		}
		it.delete();
		it = null;
		
		//System.out.println("ApproximateMemoryUsage: "+memtable.approximateMemoryUsage()+" bytes");
		//System.out.println("entry count: "+memtable.entrySize());
		
		boolean ret = false;
		Object0<Status> s = new Object0<Status>();
		ByteBuf buf = ByteBufFactory.newUnpooled();
		ret = memtable.get(new LookupKey(new UnpooledSlice("1"), 1), buf, s);
		//System.out.println("find 1, ret="+ret+", result="+s.getValue()+", value="+buf.encodeToString());
		assertTrue(ret);
		
		ret = memtable.get(new LookupKey(new UnpooledSlice("2"), 1), buf, s);
		//System.out.println("find 2, ret="+ret+", result="+s.getValue()+", value="+buf.encodeToString());
		assertTrue(ret);
		
		ret = memtable.get(new LookupKey(new UnpooledSlice("3"), 1), buf, s);
		//System.out.println("find 3, ret="+ret+", result="+s.getValue()+", value="+buf.encodeToString());
		assertTrue(ret);
		
		ret = memtable.get(new LookupKey(new UnpooledSlice("4"), 1), buf, s);
		//System.out.println("find 4, ret="+ret+", result="+s.getValue()+", value="+buf.encodeToString());
		assertTrue(ret);
		
		ret = memtable.get(new LookupKey(new UnpooledSlice("5"), 1), buf, s);
		//System.out.println("find 5, ret="+ret+", result="+s.getValue()+", value="+buf.encodeToString());
		assertTrue(ret);
		
		ret = memtable.get(new LookupKey(new UnpooledSlice("6"), 1), buf, s);
		//System.out.println("find 6, ret="+ret+", result="+s.getValue()+", value="+buf.encodeToString());
		assertFalse(ret);
		
		ret = memtable.get(new LookupKey(new UnpooledSlice("0"), 1), buf, s);
		//System.out.println("find 0, ret="+ret+", result="+s.getValue()+", value="+buf.encodeToString());
		assertFalse(ret);
	}
}
