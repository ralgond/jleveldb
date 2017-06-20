package org.ht.jleveldb;

import org.ht.jleveldb.util.Slice;

public interface SequentialFile {
	void delete();
	
	  // Read up to "n" bytes from the file.  "scratch[0..n-1]" may be
	  // written by this routine.  Sets "*result" to the data that was
	  // read (including if fewer than "n" bytes were successfully read).
	  // May set "*result" to point at data in "scratch[0..n-1]", so
	  // "scratch[0..n-1]" must be live when "*result" is used.
	  // If an error was encountered, returns a non-OK status.
	  //
	  // REQUIRES: External synchronization
	Status read(int n, Slice result, byte[] scratch);
	
	  // Skip "n" bytes from the file. This is guaranteed to be no
	  // slower that reading the same data, but may be faster.
	  //
	  // If end of file is reached, skipping will stop at the end of the
	  // file, and Skip will return OK.
	  //
	  // REQUIRES: External synchronization
	Status skip(long n);
}
