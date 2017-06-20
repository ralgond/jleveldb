package org.ht.jleveldb.db;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.ht.jleveldb.Iterator0;
import org.ht.jleveldb.Env;
import org.ht.jleveldb.FileName;
import org.ht.jleveldb.FileType;
import org.ht.jleveldb.Options;
import org.ht.jleveldb.ReadOptions;
import org.ht.jleveldb.SequentialFile;
import org.ht.jleveldb.Status;
import org.ht.jleveldb.WritableFile;
import org.ht.jleveldb.db.format.DBFormat;
import org.ht.jleveldb.db.format.InternalKey;
import org.ht.jleveldb.db.format.InternalKeyComparator;
import org.ht.jleveldb.table.Merger;
import org.ht.jleveldb.table.Table;
import org.ht.jleveldb.table.TwoLevelIterator;
import org.ht.jleveldb.util.ByteBuf;
import org.ht.jleveldb.util.ByteBufFactory;
import org.ht.jleveldb.util.Coding;
import org.ht.jleveldb.util.FuncOutput;
import org.ht.jleveldb.util.FuncOutputBoolean;
import org.ht.jleveldb.util.FuncOutputLong;
import org.ht.jleveldb.util.Mutex;
import org.ht.jleveldb.util.Slice;

public class VersionSet {
	public VersionSet(String dbname, Options options, TableCache tcache, InternalKeyComparator cmp) {
		env = options.env;
	    this.dbname = dbname;
	    this.options = options.cloneOptions();
	    this.tcache = tcache;
	    icmp = cmp;
	    nextFileNumber = 2;
	    manifestFileNumber = 0;  // Filled by Recover()
	    lastSequence = 0;
	    logNumber = 0;
	    prevLogNumber = 0;
	    descriptorFile = null;
	    descriptorLog = null;
	    dummyVersions = new Version(this);
	    current = null;
	    appendVersion(new Version(this));
	}
	
	public void delete() {
		current.unref();
		assert(dummyVersions.next == dummyVersions);  // List must be empty
		descriptorLog.delete();
		descriptorFile.delete();
	}
	
	
	void appendVersion(Version v) {
		// Make "v" current
		assert(v.refs == 0);
		assert(v != current);
		if (current != null) {
		    current.unref();
		}
		current = v;
		v.ref();
		
		// Append to linked list
		v.prev = dummyVersions.prev;
		v.next = dummyVersions;
		v.prev.next = v;
		v.next.prev = v;
	}
	
	static class Builder {
		
		static class  LevelState {
		    TreeSet<Long> deletedFiles;
		    //FileSet* added_files;
		};
		
		public Builder(VersionSet vset, Version base) {
			//TODO
		}
		
		public void delete() {
			//TODO
		}
		
		public void apply(VersionEdit edit) {
			//TODO
		}
		
		public void saveTo(Version v) {
			//TODO
		}
		
		public void maybeAddFile(Version v, int level, FileMetaData f) {
			//TODO
		}
	}
	/**
	 * Apply *edit to the current version to form a new descriptor that
	 * is both saved to persistent state and installed as the new
	 * current version.  Will release *mu while actually writing to the file.
	 * </br></br>
	 * REQUIRES: *mu is held on entry.</br>
	 * REQUIRES: no other thread concurrently calls LogAndApply()
	 * 
	 * @param edit
	 * @param mu
	 * @return
	 */
	public Status logAndApply(VersionEdit edit, Mutex mu) {
		if (edit.hasLogNumber) {
			assert(edit.logNumber >= logNumber);
			assert(edit.logNumber < nextFileNumber);
		} else {
			edit.setLogNumber(logNumber);
		}

		if (!edit.hasPrevLogNumber) {
			edit.setPrevLogNumber(prevLogNumber);
		}
		edit.setNextFile(nextFileNumber);
		edit.setLastSequence(lastSequence);

		Version v = new Version(this);
		{
			Builder builder = new Builder(this, current);
			builder.apply(edit);
			builder.saveTo(v);
		}
		finalize(v);

		// Initialize new descriptor log file if necessary by creating
		// a temporary file that contains a snapshot of the current version.
		String newManifestFile = null;
		Status s = Status.ok0();
		if (descriptorLog == null) {
			// No reason to unlock *mu here since we only hit this path in the
			// first call to LogAndApply (when opening the database).
			assert(descriptorFile == null);
			newManifestFile = FileName.getDescriptorFileName(dbname, manifestFileNumber);
			edit.setNextFile(nextFileNumber);
			FuncOutput<WritableFile> descriptorFile0 = new FuncOutput<WritableFile>();
			s = env.newWritableFile(newManifestFile, descriptorFile0);
			descriptorFile = descriptorFile0.getValue();
			if (s.ok()) {
				descriptorLog = new LogWriter(descriptorFile);
			    s = writeSnapshot(descriptorLog);
			}
		}

		// Unlock during expensive MANIFEST log write
		{
			mu.unlock();

			// Write new record to MANIFEST log
			if (s.ok()) {
				ByteBuf record = ByteBufFactory.defaultByteBuf();
			    edit.encodeTo(record);
			    s = descriptorLog.addRecord(new Slice(record));
			    if (s.ok()) {
			    	s = descriptorFile.sync();
			    }
			    if (!s.ok()) {
			        //Log(options_->info_log, "MANIFEST write: %s\n", s.ToString().c_str());
			    }
			}

			// If we just created a new descriptor file, install it by writing a
			// new CURRENT file that points to it.
			if (s.ok() && newManifestFile != null && !newManifestFile.isEmpty()) {
				s = FileName.setCurrentFile(env, dbname, manifestFileNumber);
			}

			mu.lock();
		}

		// Install the new version
		if (s.ok()) {
			appendVersion(v);
			logNumber = edit.logNumber;
			prevLogNumber = edit.prevLogNumber;
		} else {
			v.delete();
			v = null;
			if (newManifestFile != null && !newManifestFile.isEmpty()) {
				descriptorLog.delete(); //delete descriptor_log_;
			   	descriptorFile.delete(); //delete descriptor_file_;
			    descriptorLog = null;
			    descriptorFile = null;
			    env.deleteFile(newManifestFile);
			}
		}

		return s;
	}
	
	
	static class LogReporter implements LogReader.Reporter {
		Status status;
		public void corruption(int bytes, Status s) {
			if (this.status.ok()) 
				this.status = s;
		}
	};
	
	/**
	 * Recover the last saved descriptor from persistent storage.
	 * @return saveManifest
	 */
	public Status recover(FuncOutputBoolean saveManifest) {
		// Read "CURRENT" file, which contains a pointer to the current manifest file
		ByteBuf currentFileName = ByteBufFactory.defaultByteBuf();
		Status s = env.readFileToString(FileName.getCurrentFileName(dbname), currentFileName);
		if (!s.ok()) {
			return s;
		}
		
		if (currentFileName.empty() || currentFileName.getByte(currentFileName.size()-1) != (byte)(0x0A)/*\n*/) { 
			return Status.corruption("CURRENT file does not end with newline");
		}
		currentFileName.resize(currentFileName.size() - 1); //may has bugs, should test
		
		String dscname = dbname + "/" + current;
		FuncOutput<SequentialFile> file0 = new FuncOutput<SequentialFile>();
		s = env.newSequentialFile(dscname, file0);
		SequentialFile file = file0.getValue();
		if (!s.ok()) {
		    return s;
		}
		
		boolean have_log_number = false;
		boolean have_prev_log_number = false;
		boolean have_next_file = false;
		boolean have_last_sequence = false;
		long next_file = 0;
		long last_sequence = 0;
		long log_number = 0;
		long prev_log_number = 0;
		  
		Builder builder = new Builder(this, current);
		{
			LogReporter reporter = new LogReporter();
			reporter.status = s;
			LogReader reader = new LogReader(file, reporter, true/*checksum*/, 0/*initial_offset*/);
			Slice record = new Slice();
			ByteBuf scratch = ByteBufFactory.defaultByteBuf();
			while (reader.readRecord(record, scratch) && s.ok()) {
				VersionEdit edit = new VersionEdit();
			    s = edit.decodeFrom(record);
			    if (s.ok()) {
			        if (edit.hasComparator && !edit.comparator.equals(icmp.userComparator().name())) {
			        	s = Status.invalidArgument(edit.comparator + " does not match existing comparator " + icmp.userComparator().name());
			        }
			    }
			    
			    if (s.ok()) {
			        builder.apply(edit);
			    }
			    
			    if (edit.hasLogNumber) {
			        log_number = edit.logNumber;
			        have_log_number = true;
			    }
			    
			    if (edit.hasPrevLogNumber) {
			        prev_log_number = edit.prevLogNumber;
			        have_prev_log_number = true;
			    }

			    if (edit.hasNextFileNumber) {
			        next_file = edit.nextFileNumber;
			        have_next_file = true;
			    }
			    
			    if (edit.hasLastSequence) {
			        last_sequence = edit.lastSequence;
			        have_last_sequence = true;
			    }
			}
		}
		file.delete();
		file = null;
		
		
		if (s.ok()) {
		    if (!have_next_file) {
		    	s = Status.corruption("no meta-nextfile entry in descriptor");
		    } else if (!have_log_number) {
		    	s = Status.corruption("no meta-lognumber entry in descriptor");
		    } else if (!have_last_sequence) {
		    	s = Status.corruption("no last-sequence-number entry in descriptor");
		    }

		    if (!have_prev_log_number) {
		    	prev_log_number = 0;
		    }

		    markFileNumberUsed(prev_log_number);
		    markFileNumberUsed(log_number);
		}
		
		if (s.ok()) {
		    Version v = new Version(this);
		    builder.saveTo(v);
		    // Install recovered version
		    finalize(v);
		    appendVersion(v);
		    manifestFileNumber = next_file;
		    nextFileNumber = next_file + 1;
		    lastSequence = last_sequence;
		    logNumber = log_number;
		    prevLogNumber = prev_log_number;

		    // See if we can reuse the existing MANIFEST file.
		    if (reuseManifest(dscname, currentFileName.encodeToString())) {
		    	// No need to save new manifest
		    } else {
		    	saveManifest.setValue(true);
		    }
		}
		return null;
	}
	
	/**
	 * Return the current version.
	 */
	public Version current() {
		return current;
	}
	
	/**
	 * Return the current manifest file number
	 * @return
	 */
	public long manifestFileNumber() { 
		return manifestFileNumber; 
	}
	
	/**
	 * Allocate and return a new file number
	 * @return
	 */
	public long newFileNumber() {
		return nextFileNumber++; 
	}
	
	/**
	 * Arrange to reuse "file_number" unless a newer file number has
	 * already been allocated.
	 * </br></br>
	 * REQUIRES: "file_number" was returned by a call to NewFileNumber().
	 * @param file_number
	 */
	public void reuseFileNumber(long fileNumber) {
	    if (nextFileNumber == fileNumber + 1) {
	      nextFileNumber = fileNumber;
	    }
	}
	
	public int numLevelFiles(int level) {
		assert(level >= 0);
		assert(level < DBFormat.NumLevels);
		return current.files[level].size();
	}
	  
	/**
	 * Return the combined file size of all files at the specified level.
	 * @param level
	 * @return
	 */
	public long numLevelBytes(int level) {
		assert(level >= 0);
		assert(level < DBFormat.NumLevels);
		return VersionSetGlobal.totalFileSize(current.files[level]);
	}
	
	/**
	 * Return the last sequence number.
	 * @return
	 */
	public long lastSequence() {
		return lastSequence; 
	}
	
	/**
	 * Set the last sequence number to s.
	 * @param s
	 */
	public void setLastSequence(long s) {
	    assert(s >= lastSequence);
	    lastSequence = s;
	}
	
	/**
	 * Mark the specified file number as used.
	 * @param number
	 */
	public void markFileNumberUsed(long number) {
		if (nextFileNumber <= number) {
			nextFileNumber = number + 1;
		}
	}
	
	/**
	 * Return the current log file number.
	 * @return
	 */
	public long logNumber() { 
		return logNumber;
	}
	
	/**
	 * Return the log file number for the log file that is currently
	 * being compacted, or zero if there is no such log file.
	 * @return
	 */
	public long prevLogNumber() {
		return prevLogNumber;
	}
	
	/**
	 * Pick level and inputs for a new compaction.
	 * Returns NULL if there is no compaction to be done.
	 * Otherwise returns a pointer to a heap-allocated object that
	 * describes the compaction.  Caller should delete the result.
	 */
	public Compaction pickCompaction() {
		Compaction c;
		int level;

		// We prefer compactions triggered by too much data in a level over
		// the compactions triggered by seeks.
		final boolean sizeCompaction = (current.compactionScore >= 1.0);
		final boolean seekCompaction = (current.fileToCompact != null);
		if (sizeCompaction) {
		    level = current.compactionLevel;
		    assert(level >= 0);
		    assert(level+1 < DBFormat.NumLevels);
		    c = new Compaction(options, level);

		    // Pick the first file that comes after compact_pointer_[level]
		    for (int i = 0; i < current.files[level].size(); i++) {
		    	FileMetaData f = current.files[level].get(i);
		    	if (compactPointer[level].empty() ||
		    			icmp.compare(f.largest.encode(), compactPointer[level]) > 0) {
		    		c.inputs[0].add(f);
		    		break;
		    	}
		    }
		    if (c.inputs[0].isEmpty()) {
		    	// Wrap-around to the beginning of the key space
		    	c.inputs[0].add(current.files[level].get(0));
		    }
		} else if (seekCompaction) {
		    level = current.fileToCompactLevel;
		    c = new Compaction(options, level);
		    c.inputs[0].add(current.fileToCompact);
		} else {
		    return null;
		}

		c.inputVersion = current;
		c.inputVersion.ref();

		// Files in level 0 may overlap each other, so pick up all overlapping ones
		if (level == 0) {
		    InternalKey smallest = new InternalKey();
		    InternalKey largest = new InternalKey();
		    getRange(c.inputs[0], smallest, largest);
		    // Note that the next call will discard the file we placed in
		    // c->inputs_[0] earlier and replace it with an overlapping set
		    // which will include the picked file.
		    current.getOverlappingInputs(0, smallest, largest, c.inputs[0]);
		    assert(!c.inputs[0].isEmpty());
		}

		setupOtherInputs(c);

		return c;
	}
	
	/**
	 * Return a compaction object for compacting the range [begin,end] in
	 * the specified level. Caller should delete
	 * the result.
	 * 
	 * @param level
	 * @param begin
	 * @param end
	 * @return null if there is nothing in that
	 * level that overlaps the specified range.
	 */
	public static void resize(ArrayList<FileMetaData> inputs, int newSize) {
		if (inputs.size() == newSize)
			return;
		
		if (inputs.size() > newSize) {
			int delCnt = inputs.size() - newSize;
			for (int i = 0; i < delCnt; i++) {
				FileMetaData f = inputs.remove(inputs.size()-1);
				f.delete();
			}
		} else {
			int addCnt = newSize - inputs.size();
			for (int i = 0; i < addCnt; i++)
				inputs.add(new FileMetaData());
		}
	}
	public Compaction compactRange(int level, InternalKey begin, InternalKey end) {
		ArrayList<FileMetaData> inputs = new ArrayList<FileMetaData>();
		current.getOverlappingInputs(level, begin, end, inputs);
		if (inputs.isEmpty()) {
		    return null;
		}
		
		// Avoid compacting too much in one shot in case the range is large.
		// But we cannot do this for level-0 since level-0 files can overlap
		// and we must not pick one file and drop another older file if the
		// two files overlap.
		if (level > 0) {
			final long limit = VersionSetGlobal.maxFileSizeForLevel(options, level);
		    long total = 0;
		    for (int i = 0; i < inputs.size(); i++) {
		    	long s = inputs.get(i).fileSize;
		    	total += s;
		    	if (total >= limit) {
		    		resize(inputs, i + 1); //inputs.resize(i + 1)
		    		break;
		    	}
		    }
		}
		
		Compaction c = new Compaction(options, level);
		c.inputVersion = current;
		c.inputVersion.ref();
		c.inputs[0] = inputs;
		setupOtherInputs(c);
		return c;
	}
	
	/**
	 * Return the maximum overlapping data (in bytes) at next level for any
	 * file at a level >= 1.
	 * @return
	 */
	public long maxNextLevelOverlappingBytes() {
		long result = 0;
		ArrayList<FileMetaData> overlaps = new ArrayList<FileMetaData>();
		for (int level = 1; level < DBFormat.NumLevels - 1; level++) {
		    for (int i = 0; i < current.files[level].size(); i++) {
		    	FileMetaData f = current.files[level].get(i);
		    	current.getOverlappingInputs(level+1, f.smallest, f.largest, overlaps);
		    	long sum = VersionSetGlobal.totalFileSize(overlaps);
		    	if (sum > result) {
		    		result = sum;
		    	}
		    }
		}
		return result;
	}
	
//	static Iterator0 getFileIterator(Object arg, ReadOptions options, Slice fileValue) {
//		TableCache cache = (TableCache)(arg);
//		if (fileValue.size() != 16) {
//			return Iterator0.newErrorIterator(Status.corruption("FileReader invoked with unexpected value"));
//		} else {
//			return cache.newIterator(options,
//					Coding.decodeFixedNat64(fileValue.data(), fileValue.offset),
//					Coding.decodeFixedNat64(fileValue.data(), fileValue.offset + 8));
//		}
//	}
	
	
	
	/**
	 *  Create an iterator that reads over the compaction inputs for "*c".
	 *  The caller should delete the iterator when no longer needed.
	 * @param c
	 * @return
	 */
	public Iterator0 makeInputIterator(Compaction c) {
		ReadOptions opt = new ReadOptions();
		opt.verifyChecksums = options.paranoidChecks;
		opt.fillCache = false;

		// Level-0 files have to be merged together.  For other levels,
		// we will make a concatenating iterator per level.
		// TODO(opt): use concatenating iterator for level-0 if there is no overlap
		final int space = (c.level() == 0 ? c.inputs[0].size() + 1 : 2);
		Iterator0[] list = new Iterator0[space];
		int num = 0;
		for (int which = 0; which < 2; which++) {
		    if (!c.inputs[which].isEmpty()) {
		    	if (c.level() + which == 0) {
		    		final ArrayList<FileMetaData> files = c.inputs[which];
		    		for (int i = 0; i < files.size(); i++) {
		    			list[num++] = tcache.newIterator(opt, files.get(i).number, files.get(i).fileSize);
		    		}
		    	} else {
		    		// Create concatenating iterator for the files from this level
		    		list[num++] = TwoLevelIterator.newTwoLevelIterator(
		    				new Version.LevelFileNumIterator(icmp, c.inputs[which]), 
		    				VersionSetGlobal.getFileIterator, tcache, opt);
		    	}
		    }
		}
		
		assert(num <= space);
		Iterator0 result = Merger.newMergingIterator(icmp, list, num);
		list = null; //delete[] list;
		return result;
	}
	
	/**
	 *  Returns true iff some level needs a compaction.
	 * @return
	 */
	public boolean needsCompaction() {
	    Version v = current();
	    return (v.compactionScore >= 1.0) || (v.fileToCompact != null);
	}
	
	/**
	 *  Add all files listed in any live version to *live.
	 *  May also mutate some internal state.
	 */
	public void addLiveFiles(Set<Long> live) {
		for (Version v = dummyVersions.next; v != dummyVersions; v = v.next) {
			for (int level = 0; level < DBFormat.NumLevels; level++) {
				ArrayList<FileMetaData> files = v.files[level];
			    for (int i = 0; i < files.size(); i++) {
			    	live.add(files.get(i).number);
			    }
			}
		 }
	}
	
	/**
	 *  Return the approximate offset in the database of the data for
	 *  "key" as of version "v".
	 * @return
	 */
	public long approximateOffsetOf(Version v, InternalKey ikey) {
		long result = 0;
		for (int level = 0; level < DBFormat.NumLevels; level++) {
			ArrayList<FileMetaData> files = v.files[level];
		    for (int i = 0; i < files.size(); i++) {
		    	if (icmp.compare(files.get(i).largest, ikey) <= 0) {
		    		// Entire file is before "ikey", so just add the file size
		    		result += files.get(i).fileSize;
		    	} else if (icmp.compare(files.get(i).smallest, ikey) > 0) {
		    		// Entire file is after "ikey", so ignore
		    		if (level > 0) {
		    			// Files other than level 0 are sorted by meta->smallest, so
		    			// no further files in this level will contain data for
		    			// "ikey".
		    			break;
		    		}
		    	} else {
		    		// "ikey" falls in the range for this table.  Add the
		    		// approximate offset of "ikey" within the table.
		    		FuncOutput<Table> tableptr = new FuncOutput<Table>();
		    		Iterator0 iter = tcache.newIterator(
		    				new ReadOptions(), files.get(i).number, files.get(i).fileSize, tableptr);
		    		if (tableptr.getValue() != null) {
		    			result += tableptr.getValue().approximateOffsetOf(ikey.encode());
		    		}
		    		iter = null;//delete iter;
		    	}
		    }
		}
		return result;
	}
	
	class LevelSummaryStorage {
		String info;
	}
	
	public void levelSummary(LevelSummaryStorage scratch) {
		//TODO
	}
	
	
	boolean reuseManifest(String dscname, String dscbase) {
		if (!options.reuseLogs) {
			    return false;
		}
		FuncOutput<FileType> manifestType = new FuncOutput<FileType>();
		FuncOutputLong manifestNumber = new FuncOutputLong();
		FuncOutputLong manifestSize = new FuncOutputLong();
		if (!FileName.parseFileName(dscbase, manifestNumber, manifestType) ||
			      manifestType.getValue() != FileType.DescriptorFile ||
			      !env.getFileSize(dscname, manifestSize).ok() ||
			      // Make new compacted MANIFEST if old one is too big
			      manifestSize.getValue() >= VersionSetGlobal.targetFileSize(options)) {
			return false;
		}

		assert(descriptorFile == null);
		assert(descriptorLog == null);
		FuncOutput<WritableFile> descriptorFile0 = new FuncOutput<WritableFile>();
		Status r = env.newAppendableFile(dscname, descriptorFile0);
		descriptorFile = descriptorFile0.getValue();
		if (!r.ok()) {
			//Log(options_->info_log, "Reuse MANIFEST: %s\n", r.ToString().c_str());
			assert(descriptorFile == null);
			return false;
		}
		
		//Log(options_->info_log, "Reusing MANIFEST %s\n", dscname.c_str());
		descriptorLog = new LogWriter(descriptorFile, manifestSize.getValue());
		manifestFileNumber = manifestNumber.getValue();
		return true;
	}
	
	void finalize(Version v) {
		// Precomputed best level for next compaction
		int bestLevel = -1;
		double bestScore = -1.0;

		for (int level = 0; level < DBFormat.NumLevels-1; level++) {
		    double score;
		    if (level == 0) {
		      // We treat level-0 specially by bounding the number of files
		      // instead of number of bytes for two reasons:
		      //
		      // (1) With larger write-buffer sizes, it is nice not to do too
		      // many level-0 compactions.
		      //
		      // (2) The files in level-0 are merged on every read and
		      // therefore we wish to avoid too many files when the individual
		      // file size is small (perhaps because of a small write-buffer
		      // setting, or very high compression ratios, or lots of
		      // overwrites/deletions).
		      score = v.files[level].size() /
		          (double)(DBFormat.L0_CompactionTrigger);
		    } else {
		    	// Compute the ratio of current size to size limit.
		    	long level_bytes = VersionSetGlobal.totalFileSize(v.files[level]);
		    	score = (double)(level_bytes) / VersionSetGlobal.maxBytesForLevel(options, level);
		    }

		    if (score > bestScore) {
		    	bestLevel = level;
		    	bestScore = score;
		    }
		}

		v.compactionLevel = bestLevel;
		v.compactionScore = bestScore;
	}
	
	void getRange(ArrayList<FileMetaData> inputs,
            InternalKey smallest,
            InternalKey largest) {
		assert(!inputs.isEmpty());
		smallest.clear();
		largest.clear();
		for (int i = 0; i < inputs.size(); i++) {
		    FileMetaData f = inputs.get(i);
		    if (i == 0) {
		    	smallest.assgin(f.smallest); //*smallest = f.smallest;
		        largest.assgin(f.largest); //*largest = f->largest;
		    } else {
		    	if (icmp.compare(f.smallest, smallest) < 0) {
		    		smallest.assgin(f.smallest); //*smallest = f->smallest;
		    	}
		    	if (icmp.compare(f.largest, largest) > 0) {
		    		largest.assgin(f.largest); //*largest = f->largest;
		    	}
		    }
		}
	}
	
	void getRange2(ArrayList<FileMetaData> inputs1,
            List<FileMetaData> inputs2,
            InternalKey smallest,
            InternalKey largest) {
		ArrayList<FileMetaData> all = inputs1;
		all.addAll(inputs2);
		getRange(all, smallest, largest);
	}
	
	void setupOtherInputs(Compaction c) {
		int level = c.level();
		InternalKey smallest = new InternalKey();
		InternalKey largest = new InternalKey();
		getRange(c.inputs[0], smallest, largest);
		
		current.getOverlappingInputs(level+1, smallest, largest, c.inputs[1]);

		// Get entire range covered by compaction
		InternalKey allStart = new InternalKey();
		InternalKey allLimit = new InternalKey();
		getRange2(c.inputs[0], c.inputs[1], allStart, allLimit);
		
		// See if we can grow the number of inputs in "level" without
		// changing the number of "level+1" files we pick up.
		if (!c.inputs[1].isEmpty()) {
			ArrayList<FileMetaData> expanded0 = new ArrayList<FileMetaData>();
		    current.getOverlappingInputs(level, allStart, allLimit, expanded0);
		    //final long inputs0Size = VersionSetGlobal.totalFileSize(c.inputs[0]);
		    final long inputs1Size = VersionSetGlobal.totalFileSize(c.inputs[1]);
		    final long expanded0Size = VersionSetGlobal.totalFileSize(expanded0);
		    if (expanded0.size() > c.inputs[0].size() &&
		        inputs1Size + expanded0Size <
		            VersionSetGlobal.expandedCompactionByteSizeLimit(options)) {
		    	InternalKey newStart = new InternalKey();
		    	InternalKey newLimit = new InternalKey();
		    	getRange(expanded0, newStart, newLimit);
		    	ArrayList<FileMetaData> expanded1 = new ArrayList<FileMetaData>();
		    	current.getOverlappingInputs(level+1, newStart, newLimit, expanded1);
		    	if (expanded1.size() == c.inputs[1].size()) {
//		        Log(options_->info_log,
//		            "Expanding@%d %d+%d (%ld+%ld bytes) to %d+%d (%ld+%ld bytes)\n",
//		            level,
//		            int(c->inputs_[0].size()),
//		            int(c->inputs_[1].size()),
//		            long(inputs0_size), long(inputs1_size),
//		            int(expanded0.size()),
//		            int(expanded1.size()),
//		            long(expanded0_size), long(inputs1_size));
		        smallest = newStart;
		        largest = newLimit;
		        	c.inputs[0] = expanded0;
		        	c.inputs[1] = expanded1;
		        	getRange2(c.inputs[0], c.inputs[1], allStart, allLimit);
		    	}
		    }
		}

		// Compute the set of grandparent files that overlap this compaction
		// (parent == level+1; grandparent == level+2)
		if (level + 2 < DBFormat.NumLevels) {
			current.getOverlappingInputs(level + 2, allStart, allLimit, c.grandparents);
		}

//		if (false) {
//			  options.infoLog.log("Compacting %d '%s' .. '%s'",
//		        level,
//		        smallest.debugString(),
//		        largest.debugString());
//		}

		  // Update the place where we will do the next compaction for this level.
		  // We update this immediately instead of waiting for the VersionEdit
		  // to be applied so that if the compaction fails, we will try a different
		  // key range next time.
		compactPointer[level] = ByteBufFactory.defaultByteBuf(largest.rep().data(), largest.rep().size());
		c.edit.setCompactPointer(level, largest);
	}
	
	Status writeSnapshot(LogWriter log) {
		  // TODO: Break up into multiple records to reduce memory usage on recovery?

		  // Save metadata
		  VersionEdit edit = new VersionEdit();
		  edit.setComparatorName(icmp.userComparator().name());

		  // Save compaction pointers
		  for (int level = 0; level < DBFormat.NumLevels; level++) {
			  if (!compactPointer[level].empty()) {
				  InternalKey key = new InternalKey();
				  key.decodeFrom(compactPointer[level]);
				  edit.setCompactPointer(level, key);
			  }
		  }

		  // Save files
		  for (int level = 0; level < DBFormat.NumLevels; level++) {
			  ArrayList<FileMetaData> files = current.files[level];
		   	  for (int i = 0; i < files.size(); i++) {
		   		  FileMetaData f = files.get(i);
		   		  edit.addFile(level, f.number, f.fileSize, f.smallest, f.largest);
		   	  }
		  }

		  ByteBuf record = ByteBufFactory.defaultByteBuf();
		  edit.encodeTo(record);
		  return log.addRecord(new Slice(record));
	}

	
	Env env;
	String dbname;
	Options options;
	TableCache tcache;
	InternalKeyComparator icmp;
	long nextFileNumber;
	long manifestFileNumber;
	long lastSequence;
	long logNumber;
	long prevLogNumber;  // 0 or backing store for memtable being compacted
	
	// Opened lazily
	WritableFile descriptorFile;
	LogWriter descriptorLog;
	Version dummyVersions; // Head of circular doubly-linked list of versions.
	Version current; // == dummy_versions_.prev_
	
	/**
	 *  Per-level key at which the next compaction at that level should start.
	 *  Either an empty string, or a valid InternalKey.
	 */
	ByteBuf[] compactPointer = new ByteBuf[DBFormat.NumLevels];
}
