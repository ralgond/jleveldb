package org.ht.jleveldb.test;


import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.ht.jleveldb.db.format.DBFormat;
import org.ht.jleveldb.db.format.InternalKeyComparator;
import org.ht.jleveldb.db.format.ParsedInternalKey;
import org.ht.jleveldb.db.format.ValueType;
import org.ht.jleveldb.util.ByteBuf;
import org.ht.jleveldb.util.ByteBufFactory;
import org.ht.jleveldb.util.BytewiseComparatorImpl;
import org.ht.jleveldb.util.Slice;

public class TestDBFormat {
	static Slice iKey(Slice userKey,
            long seq,
            ValueType vt) {
		ByteBuf encoded = ByteBufFactory.defaultByteBuf();
		DBFormat.appendInternalKey(encoded, new ParsedInternalKey(userKey, seq, vt));
		return new Slice(encoded);
	}
	
	static Slice shorten(Slice s, Slice limit) {
		ByteBuf result = ByteBufFactory.defaultByteBuf();
		result.assign(s.data(), s.offset(), s.size());
		(new InternalKeyComparator(BytewiseComparatorImpl.getInstance())).findShortestSeparator(result, limit);
		return new Slice(result);
	}

	static Slice shortSuccessor(Slice s) {
		ByteBuf result = ByteBufFactory.defaultByteBuf();
		result.assign(s.data(), s.offset(), s.size());
		(new InternalKeyComparator(BytewiseComparatorImpl.getInstance())).findShortSuccessor(result);
		return new Slice(result);
	}
	
	public void testKey(Slice key,
            long seq,
            ValueType vt) {
		Slice encoded = iKey(key, seq, vt);

		Slice in = new Slice(encoded);
		ParsedInternalKey decoded = new ParsedInternalKey();

		assertTrue(decoded.parse(in));
		assertTrue(key.equals(decoded.userKey));
		assertEquals(seq, decoded.sequence);
		assertTrue(vt == decoded.type);
		assertFalse(decoded.parse(new Slice("bar")));
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
		        testKey(new Slice(keys[k]), seq[s], ValueType.Value);
		        testKey(new Slice("hello"), 1, ValueType.Deletion);
		    }
		}
	}
	
	@Test
	public void testInternalKeyShortSeparator() {
		// When user keys are same
		assertTrue(iKey(new Slice("foo"), 100, ValueType.Value).equals(
		            shorten(iKey(new Slice("foo"), 100, ValueType.Value),
		                    iKey(new Slice("foo"), 99, ValueType.Value))));
		assertTrue(iKey(new Slice("foo"), 100, ValueType.Value).equals(
		            shorten(iKey(new Slice("foo"), 100, ValueType.Value),
		                    iKey(new Slice("foo"), 101, ValueType.Value))));
		assertTrue(iKey(new Slice("foo"), 100, ValueType.Value).equals(
		            shorten(iKey(new Slice("foo"), 100, ValueType.Value),
		                    iKey(new Slice("foo"), 100, ValueType.Value))));
		assertTrue(iKey(new Slice("foo"), 100, ValueType.Value).equals(
		            shorten(iKey(new Slice("foo"), 100, ValueType.Value),
		                    iKey(new Slice("foo"), 100, ValueType.Deletion))));

		// When user keys are misordered
		assertTrue(iKey(new Slice("foo"), 100, ValueType.Value).equals(
		            shorten(iKey(new Slice("foo"), 100, ValueType.Value),
		                    iKey(new Slice("bar"), 99, ValueType.Value))));

		// When user keys are different, but correctly ordered
		assertTrue(iKey(new Slice("g"), DBFormat.kMaxSequenceNumber, DBFormat.kValueTypeForSeek).equals(
		            shorten(iKey(new Slice("foo"), 100, ValueType.Value),
		                    iKey(new Slice("hello"), 200, ValueType.Value))));

		// When start user key is prefix of limit user key
		assertTrue(iKey(new Slice("foo"), 100, ValueType.Value).equals(
		            shorten(iKey(new Slice("foo"), 100, ValueType.Value),
		                    iKey(new Slice("foobar"), 200, ValueType.Value))));

		// When limit user key is prefix of start user key
		assertTrue(iKey(new Slice("foobar"), 100, ValueType.Value).equals(
		            shorten(iKey(new Slice("foobar"), 100, ValueType.Value), iKey(new Slice("foo"), 200, ValueType.Value))));
	}
	
	@Test
	public void testInternalKeyShortestSuccessor() {
		assertTrue(iKey(new Slice("g"), DBFormat.kMaxSequenceNumber, DBFormat.kValueTypeForSeek).equals(
	            shortSuccessor(iKey(new Slice("foo"), 100,ValueType.Value))));
		
	  byte[] buf = new byte[] {(byte)0xff, (byte)0xff};
	  assertTrue(iKey(new Slice(buf,0,buf.length), 100, ValueType.Value).equals(
	            shortSuccessor(iKey(new Slice(buf,0,buf.length), 100, ValueType.Value))));
	}
	
}
