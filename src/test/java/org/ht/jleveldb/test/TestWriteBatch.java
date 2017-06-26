package org.ht.jleveldb.test;

import static org.junit.Assert.assertEquals;

import org.ht.jleveldb.Iterator0;
import org.ht.jleveldb.Status;
import org.ht.jleveldb.WriteBatch;
import org.ht.jleveldb.db.MemTable;
import org.ht.jleveldb.db.WriteBatchInternal;
import org.ht.jleveldb.db.format.InternalKeyComparator;
import org.ht.jleveldb.db.format.ParsedInternalKey;
import org.ht.jleveldb.util.BytewiseComparatorImpl;
import org.ht.jleveldb.util.Slice;
import org.junit.Test;

public class TestWriteBatch {

	static String printContents(WriteBatch b) {
		  InternalKeyComparator cmp = new InternalKeyComparator(BytewiseComparatorImpl.getInstance());
		  MemTable mem = new MemTable(cmp);
		  mem.ref();
		  String state = "";
		  Status s = WriteBatchInternal.insertInto(b, mem);
		  int count = 0;
		  Iterator0 iter = mem.newIterator();
		  for (iter.seekToFirst(); iter.valid(); iter.next()) {
			  Slice ikey = iter.key();
			  ParsedInternalKey pikey = new ParsedInternalKey();
			  pikey.parse(ikey);
			  switch (pikey.type) {
		      case Value:
		        state += "Put(";
		        state += ikey.encodeToString();
		        state += ", ";
		        state += iter.value().encodeToString();
		        state += ")";
		        count++;
		        break;
		      case Deletion:
		        state += "Delete(";
		        state += ikey.encodeToString();
		        state += ")";
		        count++;
		        break;
		    }
		    state += "@";
		    state += pikey.sequence;
		  }
		  iter.delete();
		  if (!s.ok()) {
			  state += "ParseError()";
		  } else if (count != WriteBatchInternal.count(b)) {
			  state += "CountMismatch()";
		  }
		  mem.unref();
		  //System.out.println(state);
		  return state;
	}

	@Test
	public void testEmpty() {
		  WriteBatch batch = new WriteBatch();
		  assertEquals("", printContents(batch));
		  assertEquals(0, WriteBatchInternal.count(batch));
	}
	
	@Test
	public void testMultiple() {
		WriteBatch batch = new WriteBatch();
		batch.put(new Slice("foo"), new Slice("bar"));
		batch.delete(new Slice("box"));
		batch.put(new Slice("baz"), new Slice("boo"));
		WriteBatchInternal.setSequence(batch, 100);
		assertEquals(100, WriteBatchInternal.sequence(batch));
		assertEquals(3, WriteBatchInternal.count(batch));
		
		assertEquals("Put(baz, boo)@102"+"Delete(box)@101"+"Put(foo, bar)@100",
		            printContents(batch));
	}
	
	@Test
	public void testCorruption() {
		WriteBatch batch = new WriteBatch();
		batch.put(new Slice("foo"), new Slice("bar"));
		batch.delete(new Slice("box"));
		WriteBatchInternal.setSequence(batch, 200);
		Slice contents = WriteBatchInternal.contents(batch);
		WriteBatchInternal.setContents(batch, new Slice(contents.data(), contents.offset, contents.size()-1));
		assertEquals("Put(foo, bar)@200ParseError()", printContents(batch));
	}
	
	@Test
	public void testAppend() {
		WriteBatch b1 = new WriteBatch();
		WriteBatch b2 = new WriteBatch();
		
		WriteBatchInternal.setSequence(b1, 200);
		WriteBatchInternal.setSequence(b2, 300);
		WriteBatchInternal.append(b1, b2);
		assertEquals("", printContents(b1));
		b2.put(new Slice("a"), new Slice("va"));
		WriteBatchInternal.append(b1, b2);
		assertEquals("Put(a, va)@200", printContents(b1));
		b2.clear();
		b2.put(new Slice("b"), new Slice("vb"));
		WriteBatchInternal.append(b1, b2);
		assertEquals("Put(a, va)@200Put(b, vb)@201", printContents(b1));
		b2.delete(new Slice("foo"));
		WriteBatchInternal.append(b1, b2);
		assertEquals("Put(a, va)@200"+
		            "Put(b, vb)@202"+
		            "Put(b, vb)@201"+
		            "Delete(foo)@203",
		            printContents(b1));
	}
}
