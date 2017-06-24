package org.ht.jleveldb.util;

public class Integer0 {
	int value;
	
	public int getValue() {
		return value;
	}
	
	public void setValue(int value) {
		this.value = value;
	}
	
	public int incrementAndGet(int n) {
		value += n;
		return value;
	}
	
	public int getAndIncrement(int n) {
		int old = value;
		value += n;
		return old;
	}
}
