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

package com.tchaicatkovsky.jleveldb.table;

import com.tchaicatkovsky.jleveldb.Iterator0;
import com.tchaicatkovsky.jleveldb.Status;
import com.tchaicatkovsky.jleveldb.util.Slice;

public class Iterator0Wrapper {
	Iterator0 iter;
	boolean valid;
	Slice key;

	
	public Iterator0Wrapper() {
		
	}
	
	public Iterator0Wrapper(Iterator0 iter0) {
		set(iter0);
	}
	
	Iterator0 iter() { 
		return iter; 
	}
	
	void update() {
	    valid = iter.valid();
	    if (valid) {
	    	key = iter.key();
	    }
	}
	
	/**
	 * Takes ownership of "iter" and will delete it when destroyed, or
	 * when set() is invoked again.
	 * 
	 * @param iter0
	 */
	void set(Iterator0 iter0) {
		if (iter != null)
			iter.delete();
	    iter = null;
	    iter = iter0;
	    if (iter == null) {
	    	valid = false;
	    } else {
	    	update();
	    }
	}
	
	public void delete() {
		if (iter != null) {
			iter.delete();
			iter = null;
		}
	}
	
	public boolean valid() {
		return valid;
	}
	
	public void seekToFirst() {
		iter.seekToFirst();
		update();
	}
	
	public void seekToLast() {
		iter.seekToLast();
		update();
	}
	
	public void seek(Slice target) {
		iter.seek(target);
		update();
	}
	
	public void next() {
		iter.next();
		update();
	}
	
	public void prev() {
		iter.prev();
		update();
	}
	
	public Slice key() {
		return iter.key();
	}
	
	public Slice value() {
		return iter.value();
	}
	
	public Status status() {
		return iter.status();
	}
}
