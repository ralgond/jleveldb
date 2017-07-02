package com.tchaicatkovsky.jleveldb.test;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;

import org.junit.Test;

import com.tchaicatkovsky.jleveldb.util.Arena;
import com.tchaicatkovsky.jleveldb.util.IntObjectPair;
import com.tchaicatkovsky.jleveldb.util.Random0;
import com.tchaicatkovsky.jleveldb.util.Slice;

public class TestArena {
	@Test
	public void testEmpty() {
		Arena a = new Arena();
		a.delete();
	}
	
	@Test
	public void testSimple() {
		ArrayList<IntObjectPair<Slice>> allocated = new ArrayList<>();
		Arena arena = new Arena();
		final int N = 100000;
		int bytes = 0;
		Random0 rnd = new Random0(301);
		
		for (int i = 0; i < N; i++) {
		    int s;
		    if (i % (N / 10) == 0) {
		      s = i;
		    } else {
		      s = (int)(rnd.oneIn(4000) ? rnd.uniform(6000) :
		          (rnd.oneIn(10) ? rnd.uniform(100) : rnd.uniform(20)));
		    }
		    if (s == 0) {
		      // Our arena disallows size 0 allocations.
		      s = 1;
		    }
		      
		    Slice r = arena.allocate(s);

		    for (int b = 0; b < s; b++) {
		      // Fill the "i"th allocation with a known bit pattern
		    	r.data()[r.offset()+b] = (byte)((i % 256) & 0xff);
		    }
		    bytes += s;
		    allocated.add(new IntObjectPair<Slice>(s, r));
		    assertTrue(arena.memoryUsage() >= bytes);
		    if (i > N/10) {
		    	assertTrue(arena.memoryUsage() <= bytes * 1.10);
		    }
		}
	}
}
