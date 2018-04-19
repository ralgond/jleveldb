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
 * This file is translated from source code file Copyright (c) 2011 The 
 * LevelDB Authors and licensed under the BSD-3-Clause license.
 */

package com.tchaicatkovsky.jleveldb.db;

import java.util.ArrayList;

import com.tchaicatkovsky.jleveldb.Options;
import com.tchaicatkovsky.jleveldb.db.format.DBFormat;
import com.tchaicatkovsky.jleveldb.db.format.InternalKeyComparator;
import com.tchaicatkovsky.jleveldb.util.Comparator0;
import com.tchaicatkovsky.jleveldb.util.Slice;

/**
 * A Compaction encapsulates information about a compaction.
 * 
 * @author thuang
 */
public class Compaction {
	
	int level;
	long maxOutputFileSize;
	Version inputVersion;
	VersionEdit edit;
	
	/**
	 * Each compaction reads inputs from "level" and "level+1"
	 */
	Object inputs[]; //size=2
	
	/**
	 * State used to check for number of of overlapping grandparent files 
	 * (parent == level + 1, grandparent == level + 2)
	 */
	ArrayList<FileMetaData> grandparents;
	
	/**
	 * Index in grandparentStarts
	 */
	int grandparentIndex;
	
	/**
	 * Some output key has been seen
	 */
	boolean seenKey;
	
	/**
	 * Bytes of overlap between current output and grandparent files
	 */
	long overlappedBytes;
	
	// State for implementing IsBaseLevelForKey

	/** 
	 * level_ptrs_ holds indices into input_version_->levels_: our state
	 * is that we are positioned at one of the file ranges for each
	 * higher level than the ones involved in this compaction (i.e. for
	 * all L >= level_ + 2).
	 */
	int levelPtrs[] = new int[DBFormat.kNumLevels]; //What should type int changed to be?
	
	public Compaction(Options options, int level) {
		this.level = level;
		maxOutputFileSize = VersionSetGlobal.maxFileSizeForLevel(options, level);
		inputVersion = null;
		edit = new VersionEdit();
		grandparentIndex = 0;
		seenKey = false;
		overlappedBytes = 0;
		
		inputs = new Object[2];
		for (int i = 0; i < inputs.length; i++)
			inputs[i] = new ArrayList<FileMetaData>();
			
		grandparents = new ArrayList<FileMetaData>();
		
		for (int i = 0; i < DBFormat.kNumLevels; i++) {
		    levelPtrs[i] = 0;
		}
	}
	
	public void delete() {
		if (inputVersion != null)
			inputVersion.unref();
	}
	
	/**
	 *  Return the level that is being compacted.  Inputs from "level"
	 *  and "level+1" will be merged to produce a set of "level+1" files.
	 */
	public int level() { 
		return level; 
	}
	
	/**
	 * Return the object that holds the edits to the descriptor done
	 * by this compaction.
	 */
	public VersionEdit edit() { 
		return edit;
	}
	
	/**
	 * "which" must be either 0 or 1
	 * @param which
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public int numInputFiles(int which) { 
		return ((ArrayList<FileMetaData>)inputs[which]).size(); 
	}
	
	/**
	 * Return the ith input file at "level()+which" ("which" must be 0 or 1).
	 */
	@SuppressWarnings("unchecked")
	public FileMetaData input(int which, int i) { 
		return ((ArrayList<FileMetaData>)inputs[which]).get(i); 
	}
	
	@SuppressWarnings("unchecked")
	public ArrayList<FileMetaData> input(int which) {
		return (ArrayList<FileMetaData>)inputs[which];
	}
	
	/**
	 * Maximum size of files to build during this compaction.
	 */
	public long maxOutputFileSize() { 
		return maxOutputFileSize; 
	}
	
	/**
	 * Is this a trivial compaction that can be implemented by just
	 * moving a single input file to the next level (no merging or splitting
	 */
	public boolean isTrivialMove() {
		VersionSet vset = inputVersion.vset;
		// Avoid a move if there is lots of overlapping grandparent data.
		// Otherwise, the move could create a parent file that will require
		// a very expensive merge later on.
		return (numInputFiles(0) == 1 && numInputFiles(1) == 0 &&
		          VersionSetGlobal.totalFileSize(grandparents) <=
		        		  VersionSetGlobal.maxGrandParentOverlapBytes(vset.options));
	}
	
	/**
	 * Add all inputs to this compaction as delete operations to edit.
	 */
	public void addInputDeletions(VersionEdit edit) {
		for (int which = 0; which < 2; which++) {
			int size = numInputFiles(which);
		    for (int i = 0; i < size; i++)
		    	edit.deleteFile(level + which, input(which, i).number);
		}
	}
	
	/**
	 * Returns true if the information we have available guarantees that
	 * the compaction is producing data in "level+1" for which no data exists
	 * in levels greater than "level+1".
	 */
	public boolean isBaseLevelForKey(Slice userKey) {
		// Maybe use binary search to find right entry instead of linear search?
		final Comparator0 userCmp = inputVersion.vset.icmp.userComparator();
		for (int lvl = level + 2; lvl < DBFormat.kNumLevels; lvl++) {
		    ArrayList<FileMetaData> files = inputVersion.levelFiles(lvl);
		    for (; levelPtrs[lvl] < files.size(); ) {
		    	FileMetaData f = files.get(levelPtrs[lvl]);
		    	if (userCmp.compare(userKey, f.largest.userKey()) <= 0) {
		    		// We've advanced far enough
		    		if (userCmp.compare(userKey, f.smallest.userKey()) >= 0) {
		    			// Key falls in this file's range, so definitely not base level
		    			return false;
		    		}
		    		break;
		    	}
		    	levelPtrs[lvl]++;
		    }
		}
		return true;
	}
	
	/**
	 * Returns true iff we should stop building the current output
	 * before processing "internalKey".
	 */
	public boolean shouldStopBefore(Slice internalKey) {
		final VersionSet vset = inputVersion.vset;
		// Scan to find earliest grandparent file that contains key.
		InternalKeyComparator icmp = vset.icmp;
		while (grandparentIndex < grandparents.size() &&
		      icmp.compare(internalKey,
		                    grandparents.get(grandparentIndex).largest.encode()) > 0) {
		    if (seenKey) {
		    	overlappedBytes += grandparents.get(grandparentIndex).fileSize;
		    }
		    grandparentIndex++;
		}
		seenKey = true;

		if (overlappedBytes > VersionSetGlobal.maxGrandParentOverlapBytes(vset.options)) {
		    // Too much overlap for current output; start new output
			overlappedBytes = 0;
		    return true;
		} else {
		    return false;
		}
	}
	
	
	
	/**
	 * Release the input version for the compaction, once the compaction
	 * is successful.
	 */
	public void releaseInputs() {
		if (inputVersion != null) {
			inputVersion.unref();
			inputVersion = null;
		}
	}
}
