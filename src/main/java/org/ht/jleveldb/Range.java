package org.ht.jleveldb;

import org.ht.jleveldb.util.Slice;

public class Range {
	public Slice start;          // Included in the range
	public Slice limit;          // Not included in the range
	
	public Range() {
		
	}
	
	public Range(Slice start, Slice limit) {
		this.start = start;
		this.limit = limit;
	}
}