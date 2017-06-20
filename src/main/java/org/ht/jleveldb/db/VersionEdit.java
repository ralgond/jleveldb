package org.ht.jleveldb.db;

import java.util.ArrayList;
import java.util.Set;
import java.util.TreeSet;

import org.ht.jleveldb.Status;
import org.ht.jleveldb.db.format.InternalKey;
import org.ht.jleveldb.util.ByteBuf;
import org.ht.jleveldb.util.IntLongPair;
import org.ht.jleveldb.util.IntObjectPair;
import org.ht.jleveldb.util.Slice;

public class VersionEdit {
	Set<IntLongPair> deletedFileSet = new TreeSet<IntLongPair>();
	String comparator;
	long logNumber;
	long prevLogNumber;
	long nextFileNumber;
	long lastSequence;
	boolean hasComparator;
	boolean hasLogNumber;
	boolean hasPrevLogNumber;
	boolean hasNextFileNumber;
	boolean hasLastSequence;
	
	ArrayList<IntObjectPair<InternalKey>> compactPointers = new ArrayList<>(); //TODO: change pointer to ?
	DeletedFileSet deletedFiles;
	ArrayList<IntObjectPair<FileMetaData>> newFiles = new ArrayList<>();
	
	public void clear() {
		//TODO
	}
	
	public void setComparatorName(Slice name) {
	    hasComparator = true;
	    comparator = name.encodeToString();
	}
	
	public void setComparatorName(String name) {
		comparator = name;
	}
	
	public void setLogNumber(long num) {
	    hasLogNumber = true;
	    logNumber = num;
	}
	
	public void setPrevLogNumber(long num) {
	    hasPrevLogNumber = true;
	    prevLogNumber = num;
	}
	
	public void setNextFile(long num) {
	    hasNextFileNumber = true;
	    nextFileNumber = num;
	}
	
	public void setLastSequence(long seq) {
	    hasLastSequence = true;
	    lastSequence = seq;
	}
	
	public void setCompactPointer(int level, InternalKey key) {
	    compactPointers.add(new IntObjectPair<InternalKey>(level, key));
	}
	
	/**
	 * Add the specified file at the specified number.</br>
	 * REQUIRES: This version has not been saved (see VersionSet::SaveTo)</br>
	 * REQUIRES: "smallest" and "largest" are smallest and largest keys in file</br>
	 * @param level
	 * @param file
	 * @param fileSize
	 * @param smallest
	 * @param largest
	 */
	public void addFile(int level, long file,
            long fileSize,
            InternalKey smallest,
            InternalKey largest) {
		FileMetaData f = new FileMetaData();
		f.number = file;
		f.fileSize = fileSize;
		f.smallest = smallest;
		f.largest = largest;
		newFiles.add(new IntObjectPair<FileMetaData>(level, f));
	}
	
	/**
	 * Delete the specified "file" from the specified "level".
	 * @param level
	 * @param file
	 */
	public void deleteFile(int level, long file) {
		deletedFiles.insert(new IntLongPair(level, file));
	}
	
	public void encodeTo(ByteBuf dst) {
		//TODO
	}
	
	public Status decodeFrom(Slice src) {
		//TODO
		return null;
	}
	
	public String debugString() {
		//TODO
		return null;
	}
	
	
}
