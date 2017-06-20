package org.ht.jleveldb.db;

import org.ht.jleveldb.Status;
import org.ht.jleveldb.WritableFile;
import org.ht.jleveldb.db.LogFormat.RecordType;
import org.ht.jleveldb.util.Slice;

public class LogWriter {
	
	WritableFile dest;
	int blockOffset;       // Current offset in block
	
	  // crc32c values for all supported record types.  These are
	  // pre-computed to reduce the overhead of computing the crc of the
	  // record type stored in the header.
	int typeCrc[] = new int[LogFormat.MaxRecordType + 1];
	  
	  // Create a writer that will append data to "*dest".
	  // "*dest" must be initially empty.
	  // "*dest" must remain live while this Writer is in use.
	public LogWriter(WritableFile file) {
		//TODO
	}
	
	  // Create a writer that will append data to "*dest".
	  // "*dest" must have initial length "dest_length".
	  // "*dest" must remain live while this Writer is in use.
	public LogWriter(WritableFile dest, long destLength) {
		//TODO
	}
	
	public Status addRecord(Slice slice) {
		//TODO
		return null;
	}
	
	//Status emitPhysicalRecord(RecordType type, const char* ptr, size_t length);
	Status emitPhysicalRecord(RecordType type, byte[] ptr, long offset, long length) {
		//TODO
		return null;
	}
	
	public void delete() {
		//TODO
	}
}
