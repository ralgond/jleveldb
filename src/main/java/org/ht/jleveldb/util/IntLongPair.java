package org.ht.jleveldb.util;

public class IntLongPair {
	public int i;
	public long l;
	
	
	public IntLongPair() {
		
	}
	
	public IntLongPair(int i, long l) {
		this.i = i;
		this.l = l;
	}
	
	@Override
	public int hashCode() {
		int h = 0;
		h = h * 31 + Integer.hashCode(i);
		h = h * 31 + Long.hashCode(l);
		return h;
	}
	
	@Override
	public boolean equals(Object o) {
		IntLongPair p = (IntLongPair)o;
		return i == p.i && l == p.l;
	}
}
