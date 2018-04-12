/**
 * Copyright (c) 2017-2018, Teng Huang <ht201509 at 163 dot com>
 * All rights reserved.
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tchaicatkovsky.jleveldb.test;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;

import org.junit.Test;

import com.tchaicatkovsky.jleveldb.db.MemTableArena;
import com.tchaicatkovsky.jleveldb.util.IntObjectPair;
import com.tchaicatkovsky.jleveldb.util.Random0;
import com.tchaicatkovsky.jleveldb.util.Slice;

public class TestArena {
	@Test
	public void testEmpty() {
		MemTableArena a = new MemTableArena();
		a.delete();
	}
	
	@Test
	public void testSimple() {
		ArrayList<IntObjectPair<Slice>> allocated = new ArrayList<>();
		MemTableArena arena = new MemTableArena();
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
