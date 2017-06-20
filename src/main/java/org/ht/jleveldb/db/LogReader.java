package org.ht.jleveldb.db;

import org.ht.jleveldb.SequentialFile;
import org.ht.jleveldb.Status;
import org.ht.jleveldb.util.ByteBuf;
import org.ht.jleveldb.util.Slice;

public class LogReader {
	// Interface for reporting errors.
	public static interface Reporter {
		void corruption(int bytes, Status status);
	}
	
	
	
	
	final SequentialFile file;
	final Reporter reporter;
	final boolean checksum;
	byte[] backingStore;  //char* const backing_store_;
	Slice buffer;
	boolean eof;   // Last Read() indicated EOF by returning < kBlockSize

	  // Offset of the last record returned by ReadRecord.
	long lastRecordOffset;
	  // Offset of the first location past the end of buffer_.
	long endOfBufferOffset_;

	  // Offset at which to start looking for the first record to return
	final long initialOffset;

	  // True if we are resynchronizing after a seek (initial_offset_ > 0). In
	  // particular, a run of kMiddleType and kLastType records can be silently
	  // skipped in this mode
	boolean resyncing;

	// Extend record types with the following special values
	enum ExtendRecordType {
	    Eof(LogFormat.MaxRecordType + 1),
	    // Returned whenever we find an invalid physical record.
	    // Currently there are three situations in which this happens:
	    // * The record has an invalid CRC (ReadPhysicalRecord reports a drop)
	    // * The record is a 0-length record (No drop is reported)
	    // * The record is below constructor's initial_offset (No drop is reported)
	    BadRecord(LogFormat.MaxRecordType + 2);
		
		private int type;
		
		private ExtendRecordType(int type) {
			this.type = type;
		}
		
		public int getType() {
			return type;
		}
	}; 
	  
	  // Create a reader that will return log records from "*file".
	  // "*file" must remain live while this Reader is in use.
	  //
	  // If "reporter" is non-NULL, it is notified whenever some data is
	  // dropped due to a detected corruption.  "*reporter" must remain
	  // live while this Reader is in use.
	  //
	  // If "checksum" is true, verify checksums if available.
	  //
	  // The Reader will start reading at the first record located at physical
	  // position >= initial_offset within the file.
	public LogReader(SequentialFile file, Reporter reporter, boolean checksum,
	         long initialOffset) {
		this.file = file;
		this.reporter = reporter;
		this.checksum = checksum;
		this.initialOffset = initialOffset;
		//TODO
	}
	
	  // Read the next record into *record.  Returns true if read
	  // successfully, false if we hit end of the input.  May use
	  // "*scratch" as temporary storage.  The contents filled in *record
	  // will only be valid until the next mutating operation on this
	  // reader or the next mutation to *scratch.
	public boolean readRecord(Slice record, ByteBuf scratch) {
		//TODO
		return false;
	}
	
	  // Returns the physical offset of the last record returned by ReadRecord.
	  //
	  // Undefined before the first call to ReadRecord.
	public long lastRecordOffset() {
		//TODO
		return 0;
	}
	
	  // Skips all blocks that are completely before "initial_offset_".
	  //
	  // Returns true on success. Handles reporting.
	boolean skipToInitialBlock() {
		//TODO
		return false;
	}
	
	  // Return type, or one of the preceding special values
	int readPhysicalRecord(Slice result) {
		//TODO: is result an output parameter?
		//TODO
		return 0;
	}
	
	  // Reports dropped bytes to the reporter.
	  // buffer_ must be updated to remove the dropped bytes prior to invocation.
	void reportCorruption(long bytes, String reason) {
		//TODO
	}
	
	void reportDrop(long bytes, Status reason) {
		//TODO
	}
	
	
}
