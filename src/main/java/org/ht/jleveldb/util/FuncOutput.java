package org.ht.jleveldb.util;

public class FuncOutput<T> {
	T value;
	
	public FuncOutput() {
		value = null;
	}
	
	public void setValue(T value) {
		this.value = value;
	}
	
	public T getValue() {
		return value;
	}
}
