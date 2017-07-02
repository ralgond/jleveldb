package com.tchaicatkovsky.jleveldb.db;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.tchaicatkovsky.jleveldb.Iterator0;
import com.tchaicatkovsky.jleveldb.Logger0;
import com.tchaicatkovsky.jleveldb.ReadOptions;
import com.tchaicatkovsky.jleveldb.Status;
import com.tchaicatkovsky.jleveldb.db.format.DBFormat;
import com.tchaicatkovsky.jleveldb.db.format.InternalKey;
import com.tchaicatkovsky.jleveldb.db.format.InternalKeyComparator;
import com.tchaicatkovsky.jleveldb.db.format.LookupKey;
import com.tchaicatkovsky.jleveldb.db.format.ParsedInternalKey;
import com.tchaicatkovsky.jleveldb.db.format.ValueType;
import com.tchaicatkovsky.jleveldb.table.Table;
import com.tchaicatkovsky.jleveldb.table.TwoLevelIterator;
import com.tchaicatkovsky.jleveldb.util.ByteBuf;
import com.tchaicatkovsky.jleveldb.util.Coding;
import com.tchaicatkovsky.jleveldb.util.Comparator0;
import com.tchaicatkovsky.jleveldb.util.DefaultSlice;
import com.tchaicatkovsky.jleveldb.util.Slice;
import com.tchaicatkovsky.jleveldb.util.Strings;

/**
 * Each Version keeps track of a set of Table files per level.  The
 * entire set of versions is maintained in a VersionSet.
 * 
 * @author Teng Huang ht201509@163.com
 */
public class Version {
	
	public interface CallBack {
		public boolean run(Object arg, int level, FileMetaData meta);
	}
	
	final VersionSet vset;
	Version next;
	Version prev;
	int refs;
	
	/**
	 * List of files per level
	 */
	//ArrayList<FileMetaData>[] files;
	Object[] files;
	
	/**
	 * Next file to compact based on seek stats.
	 */
	FileMetaData fileToCompact;
	int fileToCompactLevel;
	
	/**
	 * Level that should be compacted next and its compaction score.
	 * Score < 1 means compaction is not strictly needed.  These fields
	 * are initialized by Finalize().
	 */
	double compactionScore;
	int compactionLevel;
	
	public Version(VersionSet vset) {
		this.vset = vset;
		next = this;
		prev = this;
		refs = 0;
		fileToCompact = null;
		fileToCompactLevel = -1;
		compactionScore = -1.0;
		compactionLevel = -1;
		files = new Object[DBFormat.kNumLevels];
		for (int i = 0; i < DBFormat.kNumLevels; i++)
			files[i] = new ArrayList<FileMetaData>();
	}
	
	public void delete() {
		assert(refs == 0);

		// Remove from linked list
		prev.next = next;
		next.prev = prev;

		// Drop references to files
		for (int level = 0; level < DBFormat.kNumLevels; level++) {
		    for (int i = 0; i < levelFiles(level).size(); i++) {
		    	FileMetaData f = levelFiles(level).get(i);
		    	assert(f.refs > 0);
		    	f.refs--;
		    	if (f.refs <= 0) {
		    		//delete f;
		    		f.delete();
		    	}
		    }
		}
	}
	
    /**
     *  Reference count management (so Versions do not disappear out from
     *  under live iterators)
     */
	public void ref() {
		vset.incrVersionRef();
		++refs;
	}
	
	public void unref() {
		assert(this != vset.dummyVersions);
		assert(refs >= 1);
		vset.decrVersionRef();
		--refs;
		if (refs == 0) {
		    delete();
		}
	}
	
	public void addIterators(ReadOptions options, List<Iterator0> iters) {
		// Merge all level zero files together since they may overlap
		for (int i = 0; i < levelFiles(0).size(); i++)
		    iters.add(vset.tableCache.newIterator(options, levelFiles(0).get(i).number, levelFiles(0).get(i).fileSize));

		// For levels > 0, we can use a concatenating iterator that sequentially
		// walks through the non-overlapping files in the level, opening them
		// lazily.
		for (int level = 1; level < DBFormat.kNumLevels; level++) {
		    if (!levelFiles(level).isEmpty())
		    	iters.add(newConcatenatingIterator(options, level));
		}
	}
	
	public Iterator0 newConcatenatingIterator(ReadOptions options, int level) {
		return TwoLevelIterator.newTwoLevelIterator(
			      new LevelFileNumIterator(vset.icmp, level, levelFiles(level)), 
			      VersionSetGlobal.getFileIterator, vset.tableCache, options);
	}
	
	
	static Comparator<FileMetaData> newestFirst = new Comparator<FileMetaData>() {
		public int compare(FileMetaData o1, FileMetaData o2) {
			return -1 * Long.compare(o1.number, o2.number);
		}
	};
	
	public static class GetStats {
		public FileMetaData seekFile;
		int seekFileLevel;
	}
	
	/**
	 * Lookup the value for key.  If found, store it in {@code value}. Fills {@code stats}.</br></br>
	 * 
	 * <b>REQUIRES: lock is not held</b>
	 * @param options
	 * @param k
	 * @param value [OUTPUT]
	 * @param stats [OUTPUT]
	 * @return OK if found, else non-OK.
	 */
	public Status get(ReadOptions options, LookupKey k, ByteBuf value, GetStats stats) {
		Slice ikey = k.internalKey();
		Slice userKey = k.userKey();
		Comparator0 ucmp = vset.icmp.userComparator();
		Status s = Status.ok0();

		stats.seekFile = null;
		stats.seekFileLevel = -1;
		FileMetaData lastFileRead = null;
		int lastFileReadLevel = -1;
		
		// We can search level-by-level since entries never hop across
		// levels.  Therefore we are guaranteed that if we find data
		// in an smaller level, later levels are irrelevant.
		ArrayList<FileMetaData> tmp = new ArrayList<FileMetaData>();
		FileMetaData tmp2;
		for (int level = 0; level < DBFormat.kNumLevels; level++) {
			int numFiles = levelFiles(level).size();
		    if (numFiles == 0) 
		    	continue;

		    // Get the list of files to search in this level
		    ArrayList<FileMetaData> levelFiles = levelFiles(level);
		    if (level == 0) {
		    	// Level-0 files may overlap each other.  Find all files that
		    	// overlap user_key and process them in order from newest to oldest.
		    	//tmp.reserve(numFiles);
		    	tmp.clear();
		    	for (int i = 0; i < numFiles; i++) {
		    		FileMetaData f = levelFiles.get(i);
		    		if (ucmp.compare(userKey, f.smallest.userKey()) >= 0 &&
		    				ucmp.compare(userKey, f.largest.userKey()) <= 0) {
		    			tmp.add(f);
		    		}
		    	}
		    	
		    	if (tmp.isEmpty()) 
		    		continue; //go to next level

		    	Collections.sort(tmp, newestFirst); //std::sort(tmp.begin(), tmp.end(), NewestFirst);
		    	levelFiles = tmp; //files = &tmp[0];
		    	numFiles = tmp.size();
		    } else {
		    	// Binary search to find earliest index whose largest key >= ikey.
		    	int index = VersionSetGlobal.findFile(vset.icmp, levelFiles(level), ikey);
		    	if (index >= numFiles) {
		    		levelFiles = null;
		    		numFiles = 0;
		    	} else {
		    		tmp2 = levelFiles.get(index);
		    		if (ucmp.compare(userKey, tmp2.smallest.userKey()) < 0) {
		    			// All of "tmp2" is past any data for userKey
		    			levelFiles = null;
		    			numFiles = 0;
		    		} else {
		    			tmp.clear();
		    			tmp.add(tmp2);
		    			levelFiles = tmp;
		    			numFiles = 1;
		    		}
		    	}
		    }


		    for (int i = 0; i < numFiles; ++i) {
		    	if (lastFileRead != null && stats.seekFile == null) {
		    		// We have had more than one seek for this read.  Charge the 1st file.
		    		stats.seekFile = lastFileRead;
		    		stats.seekFileLevel = lastFileReadLevel;
		    	}

		    	FileMetaData f = levelFiles.get(i);
		    	lastFileRead = f;
		    	lastFileReadLevel = level;

		    	Saver saver = new Saver(SaverState.kNotFound, ucmp, userKey, value);
		    	s = vset.tableCache.get(options, f.number, f.fileSize, ikey, saver, valueSaver);
		    	if (!s.ok()) {
		    		return s;
		    	}
		    	
		    	switch (saver.state) {
		        case kNotFound:
		        	break;      // Keep searching in other files
		        case kFound:
		        	return s;
		        case kDeleted:
		        	s = Status.notFound();  // May throws null pointer exception, Use empty error message for speed
		        	return s;
		        case kCorrupt:
		        	s = Status.corruption("corrupted key for "+userKey.encodeToString());
		        	return s;
		    	}
		    }
		}

		return Status.notFound();
	}
	
	enum SaverState {
		kNotFound,
		kFound,
		kDeleted,
		kCorrupt
	};
	
	static class Saver {
		SaverState state;
		Comparator0 ucmp;
		Slice userKey;
		ByteBuf value;
		
		public Saver(SaverState state, Comparator0 ucmp, Slice userKey, ByteBuf value) {
			this.state = state;
			this.ucmp = ucmp;
			this.userKey = userKey;
			this.value = value;
		}
	}
	
	static Table.HandleResult valueSaver = new Table.HandleResult() {
		public void run(Object arg, Slice ikey, Slice v) {
			Saver s = (Saver)arg;
			ParsedInternalKey parsedKey = new ParsedInternalKey();
			if (!parsedKey.parse(ikey)) {
				s.state = SaverState.kCorrupt;
			} else {
				if (s.ucmp.compare(parsedKey.userKey, s.userKey) == 0) {
					s.state = (parsedKey.type == ValueType.Value) ? SaverState.kFound : SaverState.kDeleted;
					if (s.state == SaverState.kFound)
						s.value.assign(v.data(), v.offset(), v.size());
			    }
			}
		}
	};
	
	/**
	 * Adds {@code stats} into the current state.</br></br>
	 * 
	 * <b>REQUIRES: lock is held</b>
	 * @param stats
	 * @return {@code true} if a new compaction may need to be triggered, {@code false} otherwise.
	 */
	public boolean updateStats(GetStats stats) {
		FileMetaData f = stats.seekFile;
		if (f != null) {
		    f.allowedSeeks--;
		    if (f.allowedSeeks <= 0 && fileToCompact == null) {
		    	fileToCompact = f;
		    	fileToCompactLevel = stats.seekFileLevel;
		    	return true;
		    }
		}
		return false;
	}
	
	/** 
	 * Record a sample of bytes read at the specified internal key.
	 * Samples are taken approximately once every config::kReadBytesPeriod
	 * bytes.  </br></br>
	 * 
	 * Returns true if a new compaction may need to be triggered.</br></br>
	 * 
	 * <b>REQUIRES: lock is held</b>
	 */
	static CallBack stateCallback = new CallBack() {
	    public boolean run(Object arg, int level, FileMetaData f) {
	    	State0 state = (State0)(arg);
	    	state.matches++;
	    	if (state.matches == 1) {
	    		// Remember first match.
	    		state.stats.seekFile = f;
	    		state.stats.seekFileLevel = level;
	    	}
	    	// We can stop iterating once we have a second match.
	    	return state.matches < 2;
	    }
	};
	
	static class State0 {
		 public GetStats stats;  // Holds first matching file
		 public int matches;
	}
	
	public boolean recordReadSample(Slice internalKey0) {
		ParsedInternalKey ikey = new ParsedInternalKey();
		if (!ikey.parse(internalKey0)) {
			return false;
		}
		
		State0 state = new State0();
		state.matches = 0;
		forEachOverlapping(ikey.userKey, internalKey0, state, stateCallback);

		// Must have at least two matches since we want to merge across
		// files. But what if we have a single file that contains many
		// overwrites and deletions?  Should we have another mechanism for
		// finding such files?
		if (state.matches >= 2) {
			// 1MB cost is about 1 seek (see comment in Builder::Apply).
		    return updateStats(state.stats);
		}
		return false;
	}

	
	/**
	 * 
	 * 
	 * @param level
	 * @param begin null means before all keys
	 * @param end null means after all keys
	 * @param inputs
	 */
	public void getOverlappingInputs(int level, InternalKey begin, InternalKey end, List<FileMetaData> inputs) {
		  assert(level >= 0);
		  assert(level < DBFormat.kNumLevels);
		  inputs.clear();
		  Slice userBegin = new DefaultSlice();
		  Slice userEnd = new DefaultSlice();
		  if (begin != null) {
			  userBegin = begin.userKey();
		  }
		  if (end != null) {
			  userEnd = end.userKey();
		  }
		  Comparator0 userCmp = vset.icmp.userComparator();
		  for (int i = 0; i < levelFiles(level).size(); ) {
			  FileMetaData f = levelFiles(level).get(i++);
			  Slice fileStart = f.smallest.userKey();
			  Slice fileLimit = f.largest.userKey();
			  if (begin != null && userCmp.compare(fileLimit, userBegin) < 0) {
				  // "f" is completely before specified range; skip it
			  } else if (end != null && userCmp.compare(fileStart, userEnd) > 0) {
				  // "f" is completely after specified range; skip it
			  } else {
				  inputs.add(f);
				  if (level == 0) {
					  // Level-0 files may overlap each other.  So check if the newly
					  // added file has expanded the range.  If so, restart search.
					  if (begin != null && userCmp.compare(fileStart, userBegin) < 0) {
						  userBegin = fileStart;
						  inputs.clear();
						  i = 0;
					  } else if (end != null && userCmp.compare(fileLimit, userEnd) > 0) {
						  userEnd = fileLimit;
						  inputs.clear();
						  i = 0;
					  }
				  }
			  }
		  }
	}
	
	/**
	 * some part of [smallestUserKey,largestUserKey].
	 * 
	 * @param level
	 * @param smallestUserKey null represents a key smaller than all keys in the DB.
	 * @param largestUserKey null represents a key largest than all keys in the DB.
	 * @return Returns true iff some file in the specified level overlaps
	 */
	public boolean overlapInLevel(int level, Slice smallestUserKey, Slice largestUserKey) {
		return VersionSetGlobal.someFileOverlapsRange(vset.icmp, (level > 0), levelFiles(level),
				smallestUserKey, largestUserKey);
	}
	
	/**
	 *  Return the level at which we should place a new memtable compaction
	 *  result that covers the range [smallest_user_key,largest_user_key].
	 *  
	 * @param smallestUserKey
	 * @param largestUserKey
	 * @return
	 */
	public int pickLevelForMemTableOutput(Slice smallestUserKey, Slice largestUserKey) {
		int level = 0;
		if (!overlapInLevel(0, smallestUserKey, largestUserKey)) {
			System.out.println("[DEBUG] pickLevelForMemTableOutput 1");
		    
			// Push to next level if there is no overlap in next level,
		    // and the #bytes overlapping in the level after that are limited.
		    InternalKey start = new InternalKey(smallestUserKey, DBFormat.kMaxSequenceNumber, DBFormat.kValueTypeForSeek);
		    InternalKey limit = new InternalKey(largestUserKey, 0, ValueType.Deletion); //(ValueType)(0)
		    ArrayList<FileMetaData> overlaps = new ArrayList<FileMetaData>();
		    	
		    while (level < DBFormat.kMaxMemCompactLevel) {
		    	if (overlapInLevel(level + 1, smallestUserKey, largestUserKey)) {
		    		System.out.printf("[DEBUG] pickLevelForMemTableOutput 2, level=%d\n", level);
		    		break;
		    	}
		    	if (level + 2 < DBFormat.kNumLevels) {
		    		System.out.printf("[DEBUG] pickLevelForMemTableOutput 3, level=%d\n", level);
		    		// Check that file does not overlap too many grandparent bytes.
		    		getOverlappingInputs(level + 2, start, limit, overlaps);
		    		long sum = VersionSetGlobal.totalFileSize(overlaps);
		    		if (sum > VersionSetGlobal.maxGrandParentOverlapBytes(vset.options)) {
		    			System.out.println("[DEBUG] pickLevelForMemTableOutput 4");
		    			break;
		    		}
		    	}
		    	level++;
		    }
		} else {
			System.out.println("[DEBUG] pickLevelForMemTableOutput 10");
		}
		return level;
	}
	
	public int numFiles(int level) { 
		return levelFiles(level).size(); 
	}

	
	void forEachOverlapping(Slice userKey, Slice internalKey, Object arg, CallBack func) {
		// TODO(sanjay): Change Version::get() to use this function.
		Comparator0 ucmp = vset.icmp.userComparator();

		  // Search level-0 in order from newest to oldest.
		ArrayList<FileMetaData> tmp = new ArrayList<FileMetaData>();
		//tmp.reserve(files[0].size());
		for (int i = 0; i < levelFiles(0).size(); i++) {
		    FileMetaData f = levelFiles(0).get(i);
		    if (ucmp.compare(userKey, f.smallest.userKey()) >= 0 &&
		        ucmp.compare(userKey, f.largest.userKey()) <= 0) {
		      tmp.add(f);
		    }
		}
		if (!tmp.isEmpty()) {
		    Collections.sort(tmp, newestFirst); //std::sort(tmp.begin(), tmp.end(), NewestFirst);
		    for (int i = 0; i < tmp.size(); i++) {
		    	if (!func.run(arg, 0, tmp.get(i))) {
		    		return;
		    	}
		    }
		}

		// Search other levels.
		for (int level = 1; level < DBFormat.kNumLevels; level++) {
		    int numFiles = levelFiles(level).size();
		    if (numFiles == 0) 
		    	continue;

		    // Binary search to find earliest index whose largest key >= internal_key.
		    int index = VersionSetGlobal.findFile(vset.icmp, levelFiles(level), internalKey);
		    if (index < numFiles) {
		    	FileMetaData f = levelFiles(level).get(index);
		    	if (ucmp.compare(userKey, f.smallest.userKey()) < 0) {
		    		// All of "f" is past any data for user_key
		    	} else {
		    		if (!func.run(arg, level, f)) {
		    			return;
		    		}
		    	}
		    }
		}
	}
	

	

	
	@SuppressWarnings("unchecked")
	final public ArrayList<FileMetaData> levelFiles(int level) {
		return (ArrayList<FileMetaData>)files[level];
	}
	
	
	public static class LevelFileNumIterator extends Iterator0 {
		InternalKeyComparator icmp;
		ArrayList<FileMetaData> flist;
		int index;
		final int level;
		// Backing store for value().  Holds the file number and size.
		byte[] valueBuf = new byte[16];
		Slice value0 = new DefaultSlice(valueBuf, 0, valueBuf.length);
		
		public LevelFileNumIterator(InternalKeyComparator icmp, int level, ArrayList<FileMetaData> flist) {
			this.icmp = icmp;
			this.flist = flist;
			index = flist.size();
			this.level = level;
		}
		
		@Override
		public void delete() {
			super.delete();
			icmp = null;
		}
		
		@Override
		public Status status() { 
			return Status.ok0(); 
		}
		
		@Override
		public Slice key() {
		    assert(valid());
		    return flist.get(index).largest.encode();
		}
		
		@Override
		public Slice value() {
		    assert(valid());
		    Coding.encodeFixedNat64(valueBuf, 0, flist.get(index).number);
		    Coding.encodeFixedNat64(valueBuf, 8, flist.get(index).fileSize);
		    return value0;
		}
		
		
		void debug() {
			String fmdString = "{null}";
		    if (index >= 0 && index < flist.size()) {
		    	FileMetaData fmd  = flist.get(index);
		    	fmdString = fmd.debugString();
		    }
		    Logger0.debug("LevelFileNumIterator.seek, level=%d, index=%d, fmd=%s\n", 
		    		level, index, fmdString);
		}
		
		@Override
		public void next() {
		    assert(valid());
		    index++;
		    Logger0.debug("LevelFileNumIterator.next, level=%d, index=%d\n",
		    		level, index);
		    debug();
		    if (Logger0.getDebug())
		    	Thread.dumpStack();
		}
		
		@Override
		public void prev() {
		    assert(valid());
		    if (index == 0) {
		    	index = flist.size();  // Marks as invalid
		    } else {
		    	index--;
		    }
		    debug();
		}
		
		@Override
		public boolean valid() {
		    return index < flist.size();
		}
		
		@Override
		public void seek(Slice target) {
		    index = VersionSetGlobal.findFile(icmp, flist, target);
		    
		    Logger0.debug("LevelFileNumIterator.seek, level=%d, target=%s, index=%d\n", 
		    		level, Strings.escapeString(target), index);
		    debug();
		    if (Logger0.getDebug())
		    	Thread.dumpStack();
		}
		
		@Override
		public void seekToFirst() { 
			index = 0; 
		}
		
		@Override
		public void seekToLast() {
		    index = flist.isEmpty() ? 0 : flist.size() - 1;
		}
	}
	
	public String debugDataRange() {
		String s = "";
		for (int i = 0; i < DBFormat.kNumLevels; i++) {
			ArrayList<FileMetaData> files = levelFiles(i);
			s += String.format("%s, level=%d\n", this, i);
			for (int j = 0; j < files.size(); j++) {
				FileMetaData f = files.get(j);
				s += String.format("number=%d, fileSize=%d, range=[%s, %s]\n", 
						f.number, f.fileSize, f.smallest.debugString(), f.largest.debugString());
			}
		}
		return s;
	}
	
	public String debugString() {
		return "";
	}
}
