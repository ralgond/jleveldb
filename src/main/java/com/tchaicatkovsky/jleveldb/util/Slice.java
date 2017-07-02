package com.tchaicatkovsky.jleveldb.util;

public interface Slice {
	byte[] data();
	
	int offset();
	
	int limit();
	
	int size();
	
	int incrOffset(int inc);
	
	void setOffset(int offset);
	
	void init(Slice s);
	
	void init(byte[] data, int offset, int size);
	
	byte getByte(int idx);
	
	boolean empty();
	
	void clear();
	
	String encodeToString();
	
	int compare(Slice b);
	
	void removePrefix(int n);
	
	long hashCode0();
	
	Slice clone();
}
