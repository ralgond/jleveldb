package org.ht.jleveldb.test;


import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.ht.jleveldb.Iterator0;
import org.ht.jleveldb.Status;
import org.ht.jleveldb.db.MemTable;
import org.ht.jleveldb.db.format.InternalKeyComparator;
import org.ht.jleveldb.db.format.LookupKey;
import org.ht.jleveldb.db.format.ValueType;
import org.ht.jleveldb.util.ByteBuf;
import org.ht.jleveldb.util.ByteBufFactory;
import org.ht.jleveldb.util.BytewiseComparatorImpl;
import org.ht.jleveldb.util.Object0;
import org.ht.jleveldb.util.Slice;

public class TestMemTable {
	@Test
	public void test01() {
		InternalKeyComparator ikcmp = new InternalKeyComparator(BytewiseComparatorImpl.getInstance());
		
		MemTable memtable = new MemTable(ikcmp);
		memtable.add(1, ValueType.Value, new Slice("1"), new Slice("1"));
		memtable.add(1, ValueType.Value, new Slice("2"), new Slice("2"));
		memtable.add(1, ValueType.Value, new Slice("3"), new Slice("3"));
		memtable.add(1, ValueType.Value, new Slice("4"), new Slice("4"));
		memtable.add(1, ValueType.Value, new Slice("5"), new Slice("5"));
		
		String[] ary = new String[]{"1","2","3","4","5"};
		int idx = 0;
		Iterator0 it = memtable.newIterator();
		while (it.valid()) {
			Slice k = it.key();
			//System.out.println(k.encodeToString());
			assertEquals(k.encodeToString(), ary[idx++]);
			it.next();
		}
		
		//System.out.println("ApproximateMemoryUsage: "+memtable.approximateMemoryUsage()+" bytes");
		//System.out.println("entry count: "+memtable.entrySize());
		
		boolean ret = false;
		Object0<Status> s = new Object0<Status>();
		ByteBuf buf = ByteBufFactory.defaultByteBuf();
		ret = memtable.get(new LookupKey(new Slice("1"), 1), buf, s);
		//System.out.println("find 1, ret="+ret+", result="+s.getValue()+", value="+buf.encodeToString());
		assertTrue(ret);
		
		ret = memtable.get(new LookupKey(new Slice("2"), 1), buf, s);
		//System.out.println("find 2, ret="+ret+", result="+s.getValue()+", value="+buf.encodeToString());
		assertTrue(ret);
		
		ret = memtable.get(new LookupKey(new Slice("3"), 1), buf, s);
		//System.out.println("find 3, ret="+ret+", result="+s.getValue()+", value="+buf.encodeToString());
		assertTrue(ret);
		
		ret = memtable.get(new LookupKey(new Slice("4"), 1), buf, s);
		//System.out.println("find 4, ret="+ret+", result="+s.getValue()+", value="+buf.encodeToString());
		assertTrue(ret);
		
		ret = memtable.get(new LookupKey(new Slice("5"), 1), buf, s);
		//System.out.println("find 5, ret="+ret+", result="+s.getValue()+", value="+buf.encodeToString());
		assertTrue(ret);
		
		ret = memtable.get(new LookupKey(new Slice("6"), 1), buf, s);
		//System.out.println("find 6, ret="+ret+", result="+s.getValue()+", value="+buf.encodeToString());
		assertFalse(ret);
		
		ret = memtable.get(new LookupKey(new Slice("0"), 1), buf, s);
		//System.out.println("find 0, ret="+ret+", result="+s.getValue()+", value="+buf.encodeToString());
		assertFalse(ret);
	}
}
