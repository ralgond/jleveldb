package org.ht.jleveldb.table;

import org.ht.jleveldb.Iterator0;
import org.ht.jleveldb.Status;
import org.ht.jleveldb.util.Slice;

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
	}
	
	public void seekToLast() {
		iter.seekToLast();
	}
	
	public void seek(Slice target) {
		iter.seek(target);
	}
	
	public void next() {
		iter.next();
	}
	
	public void prev() {
		iter.prev();
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
