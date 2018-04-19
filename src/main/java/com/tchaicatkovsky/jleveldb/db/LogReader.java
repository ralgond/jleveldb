/**
 * Copyright (c) 2017-2018 Teng Huang <ht201509 at 163 dot com>
 * All rights reserved.
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 * This file is translated from source code file Copyright (c) 2011 
 * The LevelDB Authors and licensed under the BSD-3-Clause license.
 */

package com.tchaicatkovsky.jleveldb.db;

import com.tchaicatkovsky.jleveldb.SequentialFile;
import com.tchaicatkovsky.jleveldb.Status;
import com.tchaicatkovsky.jleveldb.util.ByteBuf;
import com.tchaicatkovsky.jleveldb.util.Coding;
import com.tchaicatkovsky.jleveldb.util.Crc32C;
import com.tchaicatkovsky.jleveldb.util.Slice;
import com.tchaicatkovsky.jleveldb.util.SliceFactory;

public class LogReader {
	/** 
	 * Interface for reporting errors.
	 */
	public static interface Reporter {
		void corruption(int bytes, Status status);
	}
	
	final SequentialFile file;
	final Reporter reporter;
	final boolean checksum;
	byte[] backingStore;
	Slice buffer;
	
	/** 
	 * Last read() indicated EOF by returning < kBlockSize
	 */
	boolean eof;   

	/**
	 *  Offset of the last record returned by readRecord.
	 */
	long lastRecordOffset;
	
	/** 
	 * Offset of the first location past the end of buffer.
	 */
	long endOfBufferOffset;

	/**
	 * Offset at which to start looking for the first record to return
	 */
	final long initialOffset;

	/**
	 * True if we are resynchronizing after a seek (initialOffset > 0). In 
	 * particular, a run of kMiddleType and kLastType records can be silently 
	 * skipped in this mode
	 */
	boolean resyncing;

	/** 
	 * Extend record types with the following special values
	 */
	enum ExtendRecordType {
	    Eof(LogFormat.kMaxRecordType + 1),
	    
	    /**
	     * Returned whenever we find an invalid physical record.</br>
	     * Currently there are three situations in which this happens:</br>
	     * The record has an invalid CRC (ReadPhysicalRecord reports a drop)</br>
	     * The record is a 0-length record (No drop is reported)</br>
	     * The record is below constructor's initialOffset (No drop is reported)</br>
	     */
	    BadRecord(LogFormat.kMaxRecordType + 2);
		
		private int type;
		
		private ExtendRecordType(int type) {
			this.type = type;
		}
		
		public int getType() {
			return type;
		}
	}; 

	/**
	 * Create a reader that will return log records from "file". 
	 * "file" must remain live while this Reader is in use.</br></br>
	 * 
	 * If "reporter" is non-null, it is notified whenever some data is 
	 * dropped due to a detected corruption.  "reporter" must remain 
	 * live while this Reader is in use.</br></br>
	 * 
	 * If "checksum" is true, verify checksums if available.</br></br>
	 * 
	 * The Reader will start reading at the first record located at physical 
	 * position >= initialOffset within the file.
	 * 
	 * @param file
	 * @param reporter
	 * @param checksum
	 * @param initialOffset
	 */
	public LogReader(SequentialFile file, Reporter reporter, boolean checksum,
	         long initialOffset) {
		this.file = file;
		this.reporter = reporter;
		this.checksum = checksum;
		
		backingStore = new byte[LogFormat.kBlockSize];
		buffer = SliceFactory.newUnpooled();
		eof = false;
		lastRecordOffset = 0;
		endOfBufferOffset = 0;
		this.initialOffset = initialOffset;
		resyncing = initialOffset > 0;
	}
	
	/**
	 * Read the next record into record.  Returns true if read
	 * successfully, false if we hit end of the input.  May use
	 * "scratch" as temporary storage.  The contents filled in record
	 * will only be valid until the next mutating operation on this
	 * reader or the next mutation to scratch.
	 * 
	 * @param record
	 * @param scratch
	 * @return
	 */
	public boolean readRecord(Slice record, ByteBuf scratch) {
		if (lastRecordOffset < initialOffset) {
		    if (!skipToInitialBlock()) {
		    	return false;
		    }
		}
		
		scratch.clear();
		record.clear();
		
		boolean inFragmentedRecord = false;
		
		// Record offset of the logical record that we're reading
		// 0 is a dummy value to make compilers happy
		long prospectiveRecordOffset = 0;

		Slice fragment = SliceFactory.newUnpooled();
		while (true) {
			int recordType = readPhysicalRecord(fragment);
			
			// ReadPhysicalRecord may have only had an empty trailer remaining in its
		    // internal buffer. Calculate the offset of the next physical record now
		    // that it has returned, properly accounting for its header size.
		    long physicalRecordOffset =
		        endOfBufferOffset - buffer.size() - LogFormat.kHeaderSize - fragment.size();
		    
		    if (resyncing) {
		        if (recordType == LogFormat.RecordType.MiddleType.getType()) {
		        	continue;
		        } else if (recordType == LogFormat.RecordType.LastType.getType()) {
		        	resyncing = false;
		        	continue;
		        } else {
		        	resyncing = false;
		        }
		    }
		    
		    if (recordType == LogFormat.RecordType.FullType.getType()) {
		    	if (inFragmentedRecord) {
		            // Handle bug in earlier versions of log::Writer where
		            // it could emit an empty kFirstType record at the tail end
		            // of a block followed by a kFullType or kFirstType record
		            // at the beginning of the next block.
		            if (scratch.empty()) {
		            	inFragmentedRecord = false;
		            } else {
		            	reportCorruption(scratch.size(), "partial record without end(1)");
		            }
		        }
		        prospectiveRecordOffset = physicalRecordOffset;
		        scratch.clear();
		        record.init(fragment);
		        lastRecordOffset = prospectiveRecordOffset;
		        return true;
		    } else if (recordType == LogFormat.RecordType.FirstType.getType()) {
		    	if (inFragmentedRecord) {
		            // Handle bug in earlier versions of log::Writer where
		            // it could emit an empty kFirstType record at the tail end
		            // of a block followed by a kFullType or kFirstType record
		            // at the beginning of the next block.
		    		if (scratch.empty()) {
		            	inFragmentedRecord = false;
		            } else {
		            	reportCorruption(scratch.size(), "partial record without end(2)");
		            }
		        }
		        prospectiveRecordOffset = physicalRecordOffset;
		        scratch.assign(fragment.data(), fragment.offset(), fragment.size());
		        inFragmentedRecord = true;
		    } else if (recordType == LogFormat.RecordType.MiddleType.getType()) {
		    	if (!inFragmentedRecord) {
		            reportCorruption(fragment.size(),  "missing start of fragmented record(1)");
		        } else {
		            scratch.append(fragment.data(), fragment.offset(), fragment.size());
		        }
		    } else if (recordType == LogFormat.RecordType.LastType.getType()) {
		    	if (!inFragmentedRecord) {
		            reportCorruption(fragment.size(), "missing start of fragmented record(2)");
		        } else {
		            scratch.append(fragment.data(), fragment.offset(), fragment.size());
		            record.init(scratch);
		            lastRecordOffset = prospectiveRecordOffset;
		            return true;
		        }
		    } else if (recordType == ExtendRecordType.Eof.getType()) {
		    	if (inFragmentedRecord) {
		            // This can be caused by the writer dying immediately after
		            // writing a physical record but before completing the next; don't
		            // treat it as a corruption, just ignore the entire logical record.
		            scratch.clear();
		        }
		        return false;
		    } else if (recordType == ExtendRecordType.BadRecord.getType()) {
		    	 if (inFragmentedRecord) {
		             reportCorruption(scratch.size(), "error in middle of record");
		             inFragmentedRecord = false;
		             scratch.clear();
		         }
		    } else {
		        reportCorruption(
		            (fragment.size() + (inFragmentedRecord ? scratch.size() : 0)),
		            "unknown record type "+recordType);
		        inFragmentedRecord = false;
		        scratch.clear();
		    }
		}
	}
	
	/**
	 * Returns the physical offset of the last record returned by ReadRecord.
	 * 
	 * Undefined before the first call to ReadRecord.
	 * @return
	 */
	final long lastRecordOffset() {
		return this.lastRecordOffset;
	}
	
	/**
	 * Skips all blocks that are completely before "initialOffset".
	 * 
	 * @return Returns true on success. Handles reporting.
	 */
	boolean skipToInitialBlock() {
		long offsetInBlock = initialOffset % LogFormat.kBlockSize;
		long blockStartLocation = initialOffset - offsetInBlock;

		// Don't search a block if we'd be in the trailer
		if (offsetInBlock > LogFormat.kBlockSize - 6) {
			offsetInBlock = 0;
			blockStartLocation += LogFormat.kBlockSize;
		}

		endOfBufferOffset = blockStartLocation;

		// Skip to start of first block that can contain the initial record
		if (blockStartLocation > 0) {
		Status skipStatus = file.skip(blockStartLocation);
		    if (!skipStatus.ok()) {
		    	reportDrop(blockStartLocation, skipStatus);
		    	return false;
		    }
		}

		return true;
	}
	
	/**
	 * @param result [OUTPUT]
	 * @return Return type, or one of the preceding special values.
	 */
	int readPhysicalRecord(Slice result) {
		while (true) {
			if (buffer.size() < LogFormat.kHeaderSize) {
				if (!eof) {
					// Last read was a full read, so this is a trailer to skip
					buffer.clear();
					Status status = file.read(LogFormat.kBlockSize, buffer, backingStore);
					endOfBufferOffset += buffer.size();
		        
					if (!status.ok()) {
						buffer.clear();
						reportDrop(LogFormat.kBlockSize, status);
						eof = true;
						return ExtendRecordType.Eof.getType();
					} else if (buffer.size() < LogFormat.kBlockSize) {
						eof = true;
					}
					continue;
				} else {	
					// Note that if buffer_ is non-empty, we have a truncated header at the
					// end of the file, which can be caused by the writer crashing in the
					// middle of writing the header. Instead of considering this an error,
					// just report EOF.
					buffer.clear();
					return ExtendRecordType.Eof.getType();
				}
			}

		    // Parse the header
			byte[] header = buffer.data();
			int headerOffset = buffer.offset();

			int a = (int)(buffer.getByte(4) & 0xff);
			int b = (int)(buffer.getByte(5) & 0xff);
			int type = (int)(buffer.getByte(6) & 0xff);
			int length = a | (b << 8);
			
		    if (LogFormat.kHeaderSize + length > buffer.size()) {
		    	int drop_size = buffer.size();
		    	buffer.clear();
		    	if (!eof) {
		    		reportCorruption(drop_size, "bad record length");
		    		return ExtendRecordType.BadRecord.getType();
		    	}
		    	// If the end of the file has been reached without reading |length| bytes
		    	// of payload, assume the writer died in the middle of writing the record.
		    	// Don't report a corruption.
		    	return ExtendRecordType.Eof.getType();
		    }

		    if (type == LogFormat.RecordType.ZeroType.getType() && length == 0) {
		    	// Skip zero length record without reporting any drops since
		    	// such records are produced by the mmap based writing code in
		    	// env_posix.cc that preallocates file regions.
		    	buffer.clear();
		    	return ExtendRecordType.BadRecord.getType();
		    }

		    // Check crc
		    if (checksum) {
		    	long expected_crc = Crc32C.unmask(Coding.decodeFixedNat32Long(header, headerOffset));
		    	long actual_crc = Crc32C.value(header, headerOffset + 6, 1 + length);
		    	if (actual_crc != expected_crc) {
		    		// Drop the rest of the buffer since "length" itself may have
		    		// been corrupted and if we trust it, we could find some
		    		// fragment of a real log record that just happens to look
		    		// like a valid log record.
		    		int dropSize = buffer.size();
		    		buffer.clear();
		    		reportCorruption(dropSize, "checksum mismatch");
		    		return ExtendRecordType.BadRecord.getType();
		    	}
		    }

		    buffer.removePrefix(LogFormat.kHeaderSize + length);

		    // Skip physical record that started before initial_offset_
		    if (endOfBufferOffset - buffer.size() - LogFormat.kHeaderSize - length < initialOffset) {
		    	result.clear();
		    	return ExtendRecordType.BadRecord.getType();
		    }

		    result.init(header, headerOffset + LogFormat.kHeaderSize, length);
		    
		    return type;
		}
	}
	
	/**
	 * Reports dropped bytes to the reporter.
	 * buffer must be updated to remove the dropped bytes prior to invocation.
	 * 
	 * @param bytes
	 * @param reason
	 */
	void reportCorruption(long bytes, String reason) {
		reportDrop(bytes, Status.corruption(reason));
	}
	
	void reportDrop(long bytes, Status reason) {
		if (reporter != null &&
			      endOfBufferOffset - buffer.size() - bytes >= initialOffset) {
			reporter.corruption((int)(bytes), reason);
		}
	}
}
