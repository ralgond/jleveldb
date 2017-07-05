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
import com.tchaicatkovsky.jleveldb.util.Object0;
import com.tchaicatkovsky.jleveldb.util.Slice;
import com.tchaicatkovsky.jleveldb.util.SliceFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestMemTable {
	@Test
	public void test01() {
		InternalKeyComparator ikcmp = new InternalKeyComparator(BytewiseComparatorImpl.getInstance());
		
		MemTable memtable = new MemTable(ikcmp);
		memtable.add(1, ValueType.Value, SliceFactory.newUnpooled("1"), SliceFactory.newUnpooled("1"));
		memtable.add(1, ValueType.Value, SliceFactory.newUnpooled("2"), SliceFactory.newUnpooled("2"));
		memtable.add(1, ValueType.Value, SliceFactory.newUnpooled("3"), SliceFactory.newUnpooled("3"));
		memtable.add(1, ValueType.Value, SliceFactory.newUnpooled("4"), SliceFactory.newUnpooled("4"));
		memtable.add(1, ValueType.Value, SliceFactory.newUnpooled("5"), SliceFactory.newUnpooled("5"));
		
		String[] ary = new String[]{"1","2","3","4","5"};
		int idx = 0;
		Iterator0 it = memtable.newIterator();
		it.seekToFirst();
		while (it.valid()) {
			Slice ikey = it.key();
			assertEquals(DBFormat.extractUserKey(ikey).encodeToString(), ary[idx++]);
			it.next();
		}
		it.delete();
		it = null;
		
		boolean ret = false;
		Object0<Status> s = new Object0<Status>();
		ByteBuf buf = ByteBufFactory.newUnpooled();
		ret = memtable.get(new LookupKey(SliceFactory.newUnpooled("1"), 1), buf, s);
		assertTrue(ret);
		
		ret = memtable.get(new LookupKey(SliceFactory.newUnpooled("2"), 1), buf, s);
		assertTrue(ret);
		
		ret = memtable.get(new LookupKey(SliceFactory.newUnpooled("3"), 1), buf, s);
		assertTrue(ret);
		
		ret = memtable.get(new LookupKey(SliceFactory.newUnpooled("4"), 1), buf, s);
		assertTrue(ret);
		
		ret = memtable.get(new LookupKey(SliceFactory.newUnpooled("5"), 1), buf, s);
		assertTrue(ret);
		
		ret = memtable.get(new LookupKey(SliceFactory.newUnpooled("6"), 1), buf, s);
		assertFalse(ret);
		
		ret = memtable.get(new LookupKey(SliceFactory.newUnpooled("0"), 1), buf, s);
		assertFalse(ret);
	}
}
