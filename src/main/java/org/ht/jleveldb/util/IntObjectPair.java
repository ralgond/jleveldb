package org.ht.jleveldb.util;

public class IntObjectPair<T> {
	public int i;
	public T obj;
	
	public IntObjectPair() {
		
	}
	
	public IntObjectPair(int i, T obj) {
		this.i = i;
		this.obj = obj;
	}
	
	@Override
	public int hashCode() {
		int h = 0;
		h = h * 31 + Integer.hashCode(i);
		h = h * 31 + obj.hashCode();
		return h;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public boolean equals(Object o) {
		IntObjectPair<T> p = (IntObjectPair<T>)o;
		return i == p.i && obj.equals(p.obj);
	}
}
