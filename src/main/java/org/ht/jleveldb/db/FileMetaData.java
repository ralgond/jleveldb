package org.ht.jleveldb.db;

import org.ht.jleveldb.db.format.InternalKey;

public class FileMetaData {
	public int refs;
	int allowedSeeks; // Seeks allowed until compaction
	long number;
	public long fileSize;    // File size in bytes;
	InternalKey smallest;       // Smallest internal key served by table
	InternalKey largest;        // Largest internal key served by table
	
	public void delete() {
		//TODO
	}
}
