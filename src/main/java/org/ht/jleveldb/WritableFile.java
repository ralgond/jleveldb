package org.ht.jleveldb;

import org.ht.jleveldb.util.Slice;

public interface WritableFile {

	Status append(Slice data);
	
	Status close();
	
	Status flush();
	
	Status sync();
	
	void delete();
}
