package com.tchaicatkovsky.jleveldb.db;

import com.tchaicatkovsky.jleveldb.db.format.InternalKey;

public class FileMetaData {
	public int refs;
	int allowedSeeks; // Seeks allowed until compaction
	public long number;
	public long fileSize;    // File size in bytes;
	public InternalKey smallest = new InternalKey();       // Smallest internal key served by table
	public InternalKey largest = new InternalKey();        // Largest internal key served by table
	public int numEntries;
	
	public FileMetaData() {
		
	}
	
	public FileMetaData(long number, long fileSize, InternalKey smallest, InternalKey largest, int numEntries) {
		this.number = number;
		this.fileSize = fileSize;
		this.smallest = smallest;
		this.largest = largest;
		this.numEntries = numEntries;
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
		if (smallest != null)
			ret.smallest = smallest.clone();
		if (largest != null)
			ret.largest = largest.clone();
		ret.numEntries = numEntries;
		return ret;
	}
	
	public String debugString() {
		String s = "{\n";
		s += ("\tnumber: " + number + "\n");
		s += ("\tfileSize: " + fileSize + "\n");
		s += ("\tsmallest: " + smallest.debugString() + "\n");
		s += ("\tlargest: " + largest.debugString() + "\n");
		s += ("\tnumEntries: " + numEntries + "\n}\n");
		return s;
	}
}
