package com.tchaicatkovsky.jleveldb.test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;

import org.junit.Test;

import com.tchaicatkovsky.jleveldb.FilterPolicy;
import com.tchaicatkovsky.jleveldb.util.BloomFilterPolicy;
import com.tchaicatkovsky.jleveldb.util.ByteBuf;
import com.tchaicatkovsky.jleveldb.util.ByteBufFactory;
import com.tchaicatkovsky.jleveldb.util.Coding;
import com.tchaicatkovsky.jleveldb.util.Slice;
import com.tchaicatkovsky.jleveldb.util.SliceFactory;

public class TestBloom {
	
	static final int kVerbose = 1;

	static Slice key(int i, byte[] buffer, int offset) {
	  Coding.encodeFixedNat32(buffer, offset, i);
	  return SliceFactory.newUnpooled(buffer, offset, 4);
	}
	
	public static class BloomRun {
		FilterPolicy policy;
		ByteBuf filter = ByteBufFactory.newUnpooled();
		ArrayList<ByteBuf> keys = new ArrayList<>();
		
		public BloomRun() {
			this.policy = BloomFilterPolicy.newBloomFilterPolicy(10);
		}
		
		public void reset() {
		    keys.clear();
		    filter.clear();
		}
		
		public void add(Slice s) {
			ByteBuf buf = ByteBufFactory.newUnpooled();
		    buf.assign(s.data(), s.offset(), s.size());
		    keys.add(buf);
		}
		
		public void build() {
		    ArrayList<Slice> keySlices = new ArrayList<>();
		    for (int i = 0; i < keys.size(); i++) {
		    	keySlices.add(SliceFactory.newUnpooled(keys.get(i)));
		    }
		    filter.clear();
		    
		    policy.createFilter(keySlices, filter);
		    keys.clear();
		    //if (kVerbose >= 2) 
		    //	dumpFilter();
		}
		
		public int filterSize() {
		    return filter.size();
		}
		
		public void dumpFilter() {
			String s = "";
		    s += "F(";
		    for (int i = 0; i+1 < filter.size(); i++) {
		      int c = (filter.getByte(i) & 0xFF);
		      for (int j = 0; j < 8; j++) {
		    	  s += String.format("%c", (c & (1 << j)) != 0 ? '1' : '.');
		      }
		    }
		    s += ")";
		    System.err.println(s);
		}
		
		public boolean matches(Slice s) {
			if (!keys.isEmpty()) {
		    	build();
		    }
		    return policy.keyMayMatch(s, SliceFactory.newUnpooled(filter));
		}
		
		public double falsePositiveRate() {
		    byte[] buffer = new byte[4];
		    int result = 0;
		    for (int i = 0; i < 10000; i++) {
		    	if (matches(key(i + 1000000000, buffer, 0))) {
		    	  result++;
		    	}
		    }
		    return result / 10000.0;
		}
	}
	
	@Test
	public void testEmptyFilter() {
		BloomRun r = new BloomRun();
		assertFalse(r.matches(SliceFactory.newUnpooled("hello")));
		assertFalse(r.matches(SliceFactory.newUnpooled("world")));
	}
	
	@Test
	public void testSmall() {
		BloomRun r = new BloomRun();
		r.add(SliceFactory.newUnpooled("hello"));
		r.add(SliceFactory.newUnpooled("world"));
		assertTrue(r.matches(SliceFactory.newUnpooled("hello")));
		assertTrue(r.matches(SliceFactory.newUnpooled("world")));
		assertFalse(r.matches(SliceFactory.newUnpooled("x")));
		assertFalse(r.matches(SliceFactory.newUnpooled("foo")));
	}
	
	static int nextLength(int length) {
		if (length < 10) {
			length += 1;
		} else if (length < 100) {
			length += 10;
		} else if (length < 1000) {
		    length += 100;
		} else {
		    length += 1000;
		}
		return length;
	}
	
	@Test
	public void testVaryingLengths() {
		byte[] buffer = new byte[4];

		// Count number of filters that significantly exceed the false positive rate
		int mediocre_filters = 0;
		int good_filters = 0;
		
		BloomRun r = new BloomRun();
		
		for (int length = 1; length <= 10000; length = nextLength(length)) {
		    r.reset();
		    for (int i = 0; i < length; i++) {
		    	r.add(key(i, buffer, 0));
		    }
		    r.build();

		    assertTrue(r.filterSize() <= ((length * 10 / 8) + 40)); // << length;
		    
		    // All added keys must match
		    for (int i = 0; i < length; i++) {
		    	assertTrue(r.matches(key(i, buffer, 0))); //<< "Length " << length << "; key " << i;
		    }

		    // Check false positive rate
		    double rate = r.falsePositiveRate();
		    if (kVerbose >= 1) {
		    	System.err.printf("False positives: %5.2f%% @ length = %6d ; bytes = %6d\n", 
		    			rate*100.0, length, r.filterSize());
		    }
		    
		    //assertTrue(rate <= 0.02);   // Must not be over 2%
		    if (rate > 0.0125) 
		    	mediocre_filters++;  // Allowed, but not too often
		    else 
		    	good_filters++;
		}
		if (kVerbose >= 1) {
			System.err.printf("Filters: %d good, %d mediocre\n",
		            good_filters, mediocre_filters);
		}
		assertTrue(mediocre_filters <= good_filters/5);
	}
	
	@Test
	public void testFalsePositiveRate() {
		BloomRun r = new BloomRun();
		System.out.println("False Positive Rate: "+r.falsePositiveRate());
	}
}
