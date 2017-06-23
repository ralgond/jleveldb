package org.ht.jleveldb.db;

import org.ht.jleveldb.db.format.InternalKey;

public class FileMetaData {
	public int refs;
	int allowedSeeks; // Seeks allowed until compaction
	public long number;
	public long fileSize;    // File size in bytes;
	InternalKey smallest = new InternalKey();       // Smallest internal key served by table
	InternalKey largest = new InternalKey();        // Largest internal key served by table
	
	public FileMetaData() {
		
	}
	
	public FileMetaData(long number, long fileSize, InternalKey smallest, InternalKey largest) {
		this.number = number;
		this.fileSize = fileSize;
		this.smallest = smallest;
		this.largest = largest;
	}
	
	public void delete() {

	}
	
	@Override
	public FileMetaData clone() {
		FileMetaData ret = new FileMetaData();
		ret.refs = refs;
		ret.allowedSeeks = allowedSeeks;
		ret.number = number;
		ret.fileSize = fileSize;
		ret.smallest = smallest.clone();
		ret.largest = largest.clone();
		return ret;
	}
}
