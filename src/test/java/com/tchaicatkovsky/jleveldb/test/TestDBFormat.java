package com.tchaicatkovsky.jleveldb.test;


import org.junit.Test;

import com.tchaicatkovsky.jleveldb.db.format.DBFormat;
import com.tchaicatkovsky.jleveldb.db.format.InternalKeyComparator;
import com.tchaicatkovsky.jleveldb.db.format.ParsedInternalKey;
import com.tchaicatkovsky.jleveldb.db.format.ValueType;
import com.tchaicatkovsky.jleveldb.util.ByteBuf;
import com.tchaicatkovsky.jleveldb.util.ByteBufFactory;
import com.tchaicatkovsky.jleveldb.util.BytewiseComparatorImpl;
import com.tchaicatkovsky.jleveldb.util.Slice;
import com.tchaicatkovsky.jleveldb.util.SliceFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestDBFormat {
	static Slice iKey(Slice userKey,
            long seq,
            ValueType vt) {
		ByteBuf encoded = ByteBufFactory.newUnpooled();
		DBFormat.appendInternalKey(encoded, new ParsedInternalKey(userKey, seq, vt));
		return SliceFactory.newUnpooled(encoded);
	}
	
	static Slice shorten(Slice s, Slice limit) {
		ByteBuf result = ByteBufFactory.newUnpooled();
		result.assign(s.data(), s.offset(), s.size());
		(new InternalKeyComparator(BytewiseComparatorImpl.getInstance())).findShortestSeparator(result, limit);
		return SliceFactory.newUnpooled(result);
	}

	static Slice shortSuccessor(Slice s) {
		ByteBuf result = ByteBufFactory.newUnpooled();
		result.assign(s.data(), s.offset(), s.size());
		(new InternalKeyComparator(BytewiseComparatorImpl.getInstance())).findShortSuccessor(result);
		return SliceFactory.newUnpooled(result);
	}
	
	public void testKey(Slice key,
            long seq,
            ValueType vt) {
		Slice encoded = iKey(key, seq, vt);

		Slice in = SliceFactory.newUnpooled(encoded);
		ParsedInternalKey decoded = new ParsedInternalKey();

		assertTrue(decoded.parse(in));
		assertTrue(key.equals(decoded.userKey));
		assertEquals(seq, decoded.sequence);
		assertTrue(vt == decoded.type);
		assertFalse(decoded.parse(SliceFactory.newUnpooled("bar")));
	}
	
	@Test
	public void testInternalKeyEncodeDecode() {
		String keys[] = new String[] { "", "k", "hello", "longggggggggggggggggggggg" };
		long[] seq = new long[] {
		    1L, 2L, 3L,
		    (1L << 8) - 1, 1L << 8, (1L << 8) + 1,
		    (1L << 16) - 1, 1L << 16, (1L << 16) + 1,
		    (1L << 32) - 1, 1L << 32, (1L << 32) + 1
		};
		
		for (int k = 0; k < keys.length; k++) {
		    for (int s = 0; s < seq.length; s++) {
		        testKey(SliceFactory.newUnpooled(keys[k]), seq[s], ValueType.Value);
		        testKey(SliceFactory.newUnpooled("hello"), 1, ValueType.Deletion);
		    }
		}
	}
	
	@Test
	public void testInternalKeyShortSeparator() {
		// When user keys are same
		assertTrue(iKey(SliceFactory.newUnpooled("foo"), 100, ValueType.Value).equals(
		            shorten(iKey(SliceFactory.newUnpooled("foo"), 100, ValueType.Value),
		                    iKey(SliceFactory.newUnpooled("foo"), 99, ValueType.Value))));
		assertTrue(iKey(SliceFactory.newUnpooled("foo"), 100, ValueType.Value).equals(
		            shorten(iKey(SliceFactory.newUnpooled("foo"), 100, ValueType.Value),
		                    iKey(SliceFactory.newUnpooled("foo"), 101, ValueType.Value))));
		assertTrue(iKey(SliceFactory.newUnpooled("foo"), 100, ValueType.Value).equals(
		            shorten(iKey(SliceFactory.newUnpooled("foo"), 100, ValueType.Value),
		                    iKey(SliceFactory.newUnpooled("foo"), 100, ValueType.Value))));
		assertTrue(iKey(SliceFactory.newUnpooled("foo"), 100, ValueType.Value).equals(
		            shorten(iKey(SliceFactory.newUnpooled("foo"), 100, ValueType.Value),
		                    iKey(SliceFactory.newUnpooled("foo"), 100, ValueType.Deletion))));

		// When user keys are misordered
		assertTrue(iKey(SliceFactory.newUnpooled("foo"), 100, ValueType.Value).equals(
		            shorten(iKey(SliceFactory.newUnpooled("foo"), 100, ValueType.Value),
		                    iKey(SliceFactory.newUnpooled("bar"), 99, ValueType.Value))));

		// When user keys are different, but correctly ordered
		assertTrue(iKey(SliceFactory.newUnpooled("g"), DBFormat.kMaxSequenceNumber, DBFormat.kValueTypeForSeek).equals(
		            shorten(iKey(SliceFactory.newUnpooled("foo"), 100, ValueType.Value),
		                    iKey(SliceFactory.newUnpooled("hello"), 200, ValueType.Value))));

		// When start user key is prefix of limit user key
		assertTrue(iKey(SliceFactory.newUnpooled("foo"), 100, ValueType.Value).equals(
		            shorten(iKey(SliceFactory.newUnpooled("foo"), 100, ValueType.Value),
		                    iKey(SliceFactory.newUnpooled("foobar"), 200, ValueType.Value))));

		// When limit user key is prefix of start user key
		assertTrue(iKey(SliceFactory.newUnpooled("foobar"), 100, ValueType.Value).equals(
		            shorten(iKey(SliceFactory.newUnpooled("foobar"), 100, ValueType.Value), iKey(SliceFactory.newUnpooled("foo"), 200, ValueType.Value))));
	}
	
	@Test
	public void testInternalKeyShortestSuccessor() {
		assertTrue(iKey(SliceFactory.newUnpooled("g"), DBFormat.kMaxSequenceNumber, DBFormat.kValueTypeForSeek).equals(
	            shortSuccessor(iKey(SliceFactory.newUnpooled("foo"), 100,ValueType.Value))));
		
	  byte[] buf = new byte[] {(byte)0xff, (byte)0xff};
	  assertTrue(iKey(SliceFactory.newUnpooled(buf,0,buf.length), 100, ValueType.Value).equals(
	            shortSuccessor(iKey(SliceFactory.newUnpooled(buf,0,buf.length), 100, ValueType.Value))));
	}
	
}
