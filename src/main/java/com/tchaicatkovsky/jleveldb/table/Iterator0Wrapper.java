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
	
	// Takes ownership of "iter" and will delete it when destroyed, or
	// when Set() is invoked again.
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
