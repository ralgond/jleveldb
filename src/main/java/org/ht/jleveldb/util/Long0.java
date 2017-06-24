package org.ht.jleveldb.util;

public class Long0 {
	long value;
	
	public long getValue() {
		return value;
	}
	
	public void setValue(long value) {
		this.value = value;
	}
	
	public long incrementAndGet(long n) {
		value += n;
		return value;
	}
	
	public long getAndIncrement(long n) {
		long old = value;
		value += n;
		return old;
	}
}
