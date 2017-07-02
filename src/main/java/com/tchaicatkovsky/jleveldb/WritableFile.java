package com.tchaicatkovsky.jleveldb;

import com.tchaicatkovsky.jleveldb.util.Slice;

public interface WritableFile {

	Status append(Slice data);
	
	Status close();
	
	Status flush();
	
	Status sync();
	
	void delete();
}
