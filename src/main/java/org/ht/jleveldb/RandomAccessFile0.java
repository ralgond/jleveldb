package org.ht.jleveldb;

import org.ht.jleveldb.util.Slice;

public interface RandomAccessFile0 {
	void delete();
	// Read up to "n" bytes from the file starting at "offset".
	  // "scratch[0..n-1]" may be written by this routine.  Sets "*result"
	  // to the data that was read (including if fewer than "n" bytes were
	  // successfully read).  May set "*result" to point at data in
	  // "scratch[0..n-1]", so "scratch[0..n-1]" must be live when
	  // "*result" is used.  If an error was encountered, returns a non-OK
	  // status.
	  //
	  // Safe for concurrent use by multiple threads.
	Status read(long offset, int n, Slice result, byte[] scratch);
	
	void close();
}
