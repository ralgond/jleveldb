package org.ht.jleveldb.util;

public class Object0<T> {
	T value;
	
	public Object0() {
		value = null;
	}
	
	public Object0(T value) {
		this.value = value;
	}
	
	public void setValue(T value) {
		this.value = value;
	}
	
	public T getValue() {
		return value;
	}
}