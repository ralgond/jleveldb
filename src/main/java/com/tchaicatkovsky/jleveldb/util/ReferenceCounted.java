package com.tchaicatkovsky.jleveldb.util;

public interface ReferenceCounted {
	
	void ref();
	
	void unref();
}
