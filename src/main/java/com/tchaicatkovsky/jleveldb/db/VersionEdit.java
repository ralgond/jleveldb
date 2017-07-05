/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tchaicatkovsky.jleveldb.db;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

import com.tchaicatkovsky.jleveldb.Status;
import com.tchaicatkovsky.jleveldb.db.format.DBFormat;
import com.tchaicatkovsky.jleveldb.db.format.InternalKey;
import com.tchaicatkovsky.jleveldb.util.ByteBuf;
import com.tchaicatkovsky.jleveldb.util.Coding;
import com.tchaicatkovsky.jleveldb.util.IntLongPair;
import com.tchaicatkovsky.jleveldb.util.IntObjectPair;
import com.tchaicatkovsky.jleveldb.util.Integer0;
import com.tchaicatkovsky.jleveldb.util.Slice;
import com.tchaicatkovsky.jleveldb.util.SliceFactory;

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
	
	ArrayList<IntObjectPair<InternalKey>> compactPointers = new ArrayList<>();
	HashSet<IntLongPair> deletedFiles = new HashSet<>();
	ArrayList<IntObjectPair<FileMetaData>> newFiles = new ArrayList<>();
	
	/**
	 * Tag numbers for serialized VersionEdit.  These numbers are written to 
	 * disk and should not be changed.
	 */
	enum Tag {
		kComparator(1),
		kLogNumber(2),
		kNextFileNumber(3),
		kLastSequence(4),
		kCompactPointer(5),
		kDeletedFile(6),
		kNewFile(7),
		// 8 was used for large value refs
		kPrevLogNumber(9);
	  
		int value;
		private Tag(int value) {
			this.value = value;
		}
	  
		public int getValue() {
			return value;
		}
	};
	
	public void clear() {
		comparator = "";
		logNumber = 0;
		prevLogNumber = 0;
		lastSequence = 0;
		nextFileNumber = 0;
		hasComparator = false;
		hasLogNumber = false;
		hasPrevLogNumber = false;
		hasNextFileNumber = false;
		hasLastSequence = false;
		deletedFiles.clear();
		newFiles.clear();
	}
	
	public void setComparatorName(Slice name) {
	    hasComparator = true;
	    comparator = name.encodeToString();
	}
	
	public void setComparatorName(String name) {
		hasComparator = true;
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
	 * REQUIRES: This version has not been saved (see {@link VersionSet::saveTo})</br>
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
            InternalKey largest,
            int numEntries) {
		FileMetaData f = new FileMetaData(file, fileSize, smallest, largest, numEntries);
		newFiles.add(new IntObjectPair<FileMetaData>(level, f));
	}
	
	/**
	 * Delete the specified "file" from the specified "level".
	 * @param level
	 * @param file
	 */
	public void deleteFile(int level, long file) {
		deletedFiles.add(new IntLongPair(level, file));
	}
	
	public void encodeTo(ByteBuf dst) {
		if (hasComparator) {
			dst.addVarNat32(Tag.kComparator.getValue());
		    dst.addLengthPrefixedSlice(SliceFactory.newUnpooled(comparator));
		}
		
		if (hasLogNumber) {
			dst.addVarNat32(Tag.kLogNumber.getValue());
			dst.addVarNat64(logNumber);
		}
		
		if (hasPrevLogNumber) {
		    dst.addVarNat32(Tag.kPrevLogNumber.getValue());
		    dst.addVarNat64(prevLogNumber);
		}
		
		if (hasNextFileNumber) {
			dst.addVarNat32(Tag.kNextFileNumber.getValue());
			dst.addVarNat64(nextFileNumber);
		}
		
		if (hasLastSequence) {
			dst.addVarNat32(Tag.kLastSequence.getValue());
			dst.addVarNat64(lastSequence);
		}

		for (int i = 0; i < compactPointers.size(); i++) {
			dst.addVarNat32(Tag.kCompactPointer.getValue());
			dst.addVarNat32(compactPointers.get(i).i);  // level
		    dst.addLengthPrefixedSlice(compactPointers.get(i).obj.encode());
		}

		for (IntLongPair pair : deletedFiles) {
			dst.addVarNat32(Tag.kDeletedFile.getValue());
			dst.addVarNat32(pair.i);   // level
			dst.addVarNat32((int)(pair.l & 0xffffffffL));  // file number
		}

		for (int i = 0; i < newFiles.size(); i++) {
			FileMetaData f = newFiles.get(i).obj;
			dst.addVarNat32(Tag.kNewFile.getValue());
			dst.addVarNat32(newFiles.get(i).i);  // level
			dst.addVarNat64(f.number);
		    dst.addVarNat64(f.fileSize);
		    dst.addLengthPrefixedSlice(f.smallest.encode());
		    dst.addLengthPrefixedSlice(f.largest.encode());
		}
	}
	
	public Status decodeFrom(Slice src) {
		clear();
		Slice input = src.clone();
		String msg = null;
		int tag;

		// Temporary storage for parsing
		int level;
		long number;
		
		Slice str = SliceFactory.newUnpooled();
		
		Integer0 result0 = new Integer0();
		
		while (msg == null) {
			try {
				tag = Coding.popVarNat32(input);
			} catch (Exception e) {
				break;
			}
			
			if (tag == Tag.kComparator.getValue()) {
		        if (Coding.popLengthPrefixedSlice(input, str)) {
		        	comparator = str.toString();
		        	hasComparator = true;
		        } else {
		        	msg = "comparator name";
		        }
			} else if (tag == Tag.kLogNumber.getValue()) {
				try { 
					logNumber = Coding.popVarNat64(input);
					hasLogNumber = true;
				} catch (Exception e) {
					msg = "log number";
				}
			} else if (tag == Tag.kPrevLogNumber.getValue()) {
				try { 
					prevLogNumber = Coding.popVarNat64(input);
					hasPrevLogNumber = true;
				} catch (Exception e) {
					msg = "previous log number";
		        }
			} else if (tag == Tag.kNextFileNumber.getValue()) {
				try { 
					nextFileNumber = Coding.popVarNat64(input);
					hasNextFileNumber = true;
				} catch (Exception e) {
					msg = "next file number";
				}
			} else if (tag == Tag.kLastSequence.getValue()) {
				try { 
					lastSequence = Coding.popVarNat64(input);
					hasLastSequence = true;
				} catch (Exception e) {
					msg = "last sequence number";
				}
			} else if (tag == Tag.kCompactPointer.getValue()) {
				result0.setValue(0);
				InternalKey key = new InternalKey();
		        if (getLevel(input, result0) && getInternalKey(input, key)) {
		        	level = result0.getValue();
		        	compactPointers.add(new IntObjectPair<InternalKey>(level, key));
		        } else {
		        	msg = "compaction pointer";
		        }
			} else if (tag == Tag.kDeletedFile.getValue()) {
				result0.setValue(0);
				try {
					if (getLevel(input, result0)) {
						level = result0.getValue();
						number = Coding.popVarNat64(input);
						deletedFiles.add(new IntLongPair(level, number));
					} else {
						msg = "deleted file";
					}
		        } catch (Exception e) {
		        	msg = "deleted file";
		        }
			} else if (tag == Tag.kNewFile.getValue()) {
				result0.setValue(0);
				FileMetaData f = new FileMetaData();
				try {
					if (getLevel(input, result0)) {
						level = result0.getValue();
						f.number = Coding.popVarNat64(input);
						f.fileSize = Coding.popVarNat64(input);
						if (getInternalKey(input, f.smallest) && getInternalKey(input, f.largest)) {
							newFiles.add(new IntObjectPair<FileMetaData>(level, f));
						} else {
							msg = "new-file entry";
						}
					} else {
						msg = "new-file entry";
					}
				} catch (Exception e) {
					e.printStackTrace();
		        	msg = "new-file entry";
		        }
			} else {
		        msg = "unknown tag: "+tag;
		    }
		}

		if (msg == null && !input.empty()) {
			msg = "invalid tag";
		}

		Status result = Status.ok0();
		if (msg != null) {
		    result = Status.corruption("VersionEdit "+msg);
		}
		  
		return result;
	}
	
	public String debugString() {
		StringBuilder sb = new StringBuilder();
		sb.append("VersionEdit {");
		if (hasComparator) {
			sb.append("\n  Comparator: ");
			sb.append(comparator);
		}
		if (hasLogNumber) {
			sb.append("\n  LogNumber: ");
			sb.append(logNumber);
		}
		if (hasPrevLogNumber) {
			sb.append("\n  PrevLogNumber: ");
			sb.append(this.prevLogNumber);
		}
		if (hasNextFileNumber) {
			sb.append("\n  NextFile: ");
			sb.append(nextFileNumber);
		}
		if (this.hasLastSequence) {
			sb.append("\n  LastSeq: ");
			sb.append(lastSequence);
		}
		for (int i = 0; i < compactPointers.size(); i++) {
			sb.append("\n  CompactPointer: ");
			sb.append(compactPointers.get(i).i);
			sb.append(" ");
			sb.append(compactPointers.get(i).obj.debugString());
		}
		for (IntLongPair pair : deletedFileSet) {
			sb.append("\n  DeleteFile: ");
			sb.append(pair.i);
			sb.append(" ");
			sb.append(pair.l);
		}
		for (int i = 0; i < newFiles.size(); i++) {
			FileMetaData f = newFiles.get(i).obj;
			sb.append("\n  AddFile: ");
			sb.append(newFiles.get(i).i);
			sb.append(" ");
			sb.append(f.number);
			sb.append(" ");
			sb.append(f.fileSize);
			sb.append(" ");
			sb.append(f.smallest.debugString());
			sb.append(" .. ");
			sb.append(f.largest.debugString());
		}
		sb.append("\n}\n");
		return sb.toString();
	}
	
	static boolean getInternalKey(Slice input, InternalKey dst) {
		Slice str = SliceFactory.newUnpooled();
		if (Coding.popLengthPrefixedSlice(input, str)) {
		    dst.decodeFrom(str);
		    return true;
		} else {
		    return false;
		}
	}
	
	static boolean getLevel(Slice input, Integer0 level) {
		int v = Coding.popVarNat32(input);
		if (v < DBFormat.kNumLevels) {
		    level.setValue(v);
		    return true;
		} else {
		    return false;
		}
	}
}
