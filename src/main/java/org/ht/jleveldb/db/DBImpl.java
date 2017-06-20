package org.ht.jleveldb.db;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;

import org.ht.jleveldb.Iterator0;
import org.ht.jleveldb.Logger0;
import org.ht.jleveldb.DB;
import org.ht.jleveldb.Env;
import org.ht.jleveldb.FileLock0;
import org.ht.jleveldb.FileName;
import org.ht.jleveldb.FileType;
import org.ht.jleveldb.Options;
import org.ht.jleveldb.ReadOptions;
import org.ht.jleveldb.SequentialFile;
import org.ht.jleveldb.Status;
import org.ht.jleveldb.WritableFile;
import org.ht.jleveldb.WriteBatch;
import org.ht.jleveldb.WriteOptions;
import org.ht.jleveldb.db.format.DBFormat;
import org.ht.jleveldb.db.format.InternalFilterPolicy;
import org.ht.jleveldb.db.format.InternalKey;
import org.ht.jleveldb.db.format.InternalKeyComparator;
import org.ht.jleveldb.db.format.LookupKey;
import org.ht.jleveldb.db.format.ParsedInternalKey;
import org.ht.jleveldb.db.format.ValueType;
import org.ht.jleveldb.table.Merger;
import org.ht.jleveldb.table.TableBuilder;
import org.ht.jleveldb.util.ByteBuf;
import org.ht.jleveldb.util.ByteBufFactory;
import org.ht.jleveldb.util.Comparator0;
import org.ht.jleveldb.util.CondVar;
import org.ht.jleveldb.util.FuncOutput;
import org.ht.jleveldb.util.FuncOutputBoolean;
import org.ht.jleveldb.util.FuncOutputInt;
import org.ht.jleveldb.util.FuncOutputLong;
import org.ht.jleveldb.util.Mutex;
import org.ht.jleveldb.util.Slice;

public class DBImpl implements DB {

	/**
	 * Information kept for every waiting writer
	 */
	public static class Writer {
		Status status;
		WriteBatch batch;
		boolean sync;
		boolean done;
		Mutex lock;
		CondVar cv;
		
		public Writer(Mutex lock) {
			
		}
	}
	
	public static class CompactionState {
		final Compaction compaction;
		
	    /** 
	     * Sequence numbers < smallest_snapshot are not significant since we
	     * will never have to service a snapshot below smallest_snapshot.</br>
	     * Therefore if we have seen a sequence number S <= smallest_snapshot,
	     * we can drop all entries for the same key with sequence numbers < S. 
	     */
		long smallestSnapshot;
		
		/**
		 * Files produced by compaction
		 */
		public static class Output {
			long number;
			long fileSize;
			InternalKey smallest;
			InternalKey largest;
		}
		ArrayList<Output> outputs;
		
		/**
		 * State kept for output being generated
		 */
		WritableFile outFile;
		TableBuilder builder;
		
		long totalBytes;
		
		public Output currentOutput() {
			return outputs.get(outputs.size()-1);
		}
		
		public CompactionState(Compaction compaction) {
			this.compaction = compaction;
			totalBytes = 0;
		}
	}
	
	public Status Open(Options options, String name) {
		mutex.lock();
		VersionEdit edit = new VersionEdit();
		FuncOutputBoolean saveManifest = new FuncOutputBoolean();
		saveManifest.setValue(false);
		Status s = recover(edit, saveManifest);
		if (s.ok() && memtable == null) {
			// Create new log and a corresponding memtable.
			long newLogNumber = versions.newFileNumber();
			
			FuncOutput<WritableFile> result = new FuncOutput<WritableFile>();
			s = env.newWritableFile(dbname, result);
			if (s.ok()) {
				edit.setLogNumber(newLogNumber);
				logFile = result.getValue();
				logFileNumber = newLogNumber;
				logWriter = new LogWriter(logFile);
				this.memtable = new MemTable(this.internalComparator);
			}
		}
		
		if (s.ok() && saveManifest.getValue()) {
			edit.setPrevLogNumber(0); //No older logs needed after recovery.
			edit.setLogNumber(logFileNumber);
			s = versions.logAndApply(edit, mutex);
		}
		
		if (s.ok()) {
			deleteObsoleteFiles();
			maybeScheduleCompaction();
		}
		
		mutex.unlock();
		
		return s;
	}
	
	@Override
	public Status put(WriteOptions options, Slice key, Slice value) {
		WriteBatch batch = new WriteBatch();
		batch.put(key, value);
		return write(options, batch);
	}

	@Override
	public Status delete(WriteOptions options, Slice key) {
		WriteBatch batch = new WriteBatch();
		batch.delete(key);
		return write(options, batch);
	}

	@Override
	public Status write(WriteOptions options, WriteBatch batch) {
		Writer w = new Writer(mutex);
		w.batch = batch;
		w.sync = options.isSync();
		w.done  = false;
		
		mutex.lock();
		try {
			writers.add(w);
			while (!w.done && w != writers.peekFirst()) {
				w.cv.await();
			}
			
			if (w.done)
				return w.status;
			
			Status status = makeRoomForWrite(batch == null);
			long lastSequence = versions.lastSequence();
			FuncOutput<Writer> lastWriter = new FuncOutput<Writer>();
			lastWriter.setValue(w);
			if (status.ok() && batch != null) {
				// null batch is for compactions
				WriteBatch updates = buildBatchGroup(lastWriter);
				WriteBatchInternal.setSequence(updates, lastSequence + 1);
				lastSequence += WriteBatchInternal.count(updates);
				
				// Add to log and apply to memtable.  We can release the lock
			    // during this phase since &w is currently responsible for logging
			    // and protects against concurrent loggers and concurrent writes
			    // into mem.
				{
				      mutex.unlock();
				      status = logWriter.addRecord(WriteBatchInternal.contents(updates));
				      boolean syncError = false;
				      if (status.ok() && options.sync) {
				    	  status = logFile.sync();
				    	  if (!status.ok()) {
				    		  syncError = true;
				    	  }
				      }
				      
				      if (status.ok()) {
				    	  status = WriteBatchInternal.insertInto(updates, memtable);
				      }
				      
				      mutex.lock();
				      if (syncError) {
				          // The state of the log file is indeterminate: the log record we
				    	  // just added may or may not show up when the DB is re-opened.
				    	  // So we force the DB into a mode where all future writes fail.
				    	  recordBackgroundError(status);
				      }
				}
			}
			
			//TODO
//			if (updates == tmpBatch)
//				tmpBatch.clear();
			
			versions.setLastSequence(lastSequence);
			
			while (true) {
				Writer ready = writers.pollFirst();
				if (ready != w) {
					ready.status = status;
					ready.done = true;
					ready.cv.signal();
				}
				if (ready == lastWriter.getValue())
					break;
			}
			
			// Notify new head of write queue
			if (!writers.isEmpty()) {
				writers.peekFirst().cv.signal();
			}
			
			return status;
		} catch (Exception e) {
			//TODO: the w.cv
			return null;
		} finally {
			mutex.unlock();
		}
	}

	@Override
	public Status get(ReadOptions options, Slice key, ByteBuf value) {
		Status s = Status.defaultStatus();
		mutex.lock();
		try {
			long snapshotSeqNumber= 0;
			if (options.snapshot != null) {
				//TODO
//			    snapshot = reinterpret_cast<const SnapshotImpl*>(options.snapshot)->number_;
			} else {
				snapshotSeqNumber = versions.lastSequence();
			}
			
			MemTable mem0 = memtable;
			MemTable imm0 = immtable;
			Version current = versions.current();
			//mem->Ref();
			//if (imm != NULL) imm->Ref();
			//current->Ref();
			
			boolean haveStatUpdate = false;
			Version.GetStats stats = new Version.GetStats();
			{
				mutex.unlock();
				// First look in the memtable, then in the immutable memtable (if any).
			    LookupKey lkey = new LookupKey(key, snapshotSeqNumber);
			    if (mem0.get(lkey, value, s)) {
			    	// Done
			    } else if (imm0 != null && immtable.get(lkey, value, s)) {
			    	// Done
			    } else {
			    	s = current.get(options, lkey, value, stats);
			    	haveStatUpdate = true;
			    }
				mutex.lock();
			}
			
			if (haveStatUpdate && current.updateStats(stats)) {
				maybeScheduleCompaction();
			}
			
			// mem->Unref();
			// if (imm != NULL) imm->Unref();
			// current->Unref();
			return s;
		} finally {
			mutex.unlock();
		}
	}

	
	CompactionState compactionState;
	Writer writer;
	
	static class IterState {
		Mutex mutex;
		Version version;
		MemTable mem;
		MemTable imm;
	};
		
	static class CleanupIteratorState implements Runnable {
		IterState state;
		
		public CleanupIteratorState(IterState state) {
			this.state = state;
		}
		
		public void run() {
			state.mutex.lock();
			try {
				state.mem = null; // state->mem->Unref();
				state.imm = null; // if (state->imm != NULL) state->imm->Unref();
				state.version = null;
			} finally {
				state.mutex.unlock();
			}
		}
	}
	
	Iterator0 newInternalCursor(ReadOptions options, FuncOutputLong latestSnapshot, FuncOutputInt outSeed) {
		IterState cleanup = new IterState();
		mutex.lock();
		try {
			latestSnapshot.setValue(versions.lastSequence());
			
			ArrayList<Iterator0> cursorList = new ArrayList<>();
			cursorList.add(memtable.newIterator());
			MemTable mem0 = memtable; //mem_->Ref();
			MemTable imm0 = null;
			if (immtable != null) {
				cursorList.add(immtable.newIterator());
				imm0 = immtable; //imm_->Ref();
			}
			
			versions.current().addIterators(options, cursorList);
			Iterator0[] cursorArray = (Iterator0[])cursorList.toArray();
			Iterator0 internalCursor = Merger.newMergingIterator(internalComparator, cursorArray, cursorArray.length);
			Version version0 = versions.current(); //versions_->current()->Ref();
			
			cleanup.mutex = mutex;
			cleanup.mem = mem0;
			cleanup.imm = imm0;
			cleanup.version = version0;
			internalCursor.registerCleanup(new CleanupIteratorState(cleanup));
			
			outSeed.setValue(++seed);
			
			return internalCursor;
		} finally {
			mutex.unlock();
		}
	}
	
	Status newDB() {
		VersionEdit ndb = new VersionEdit();
		ndb.setComparatorName(userComparator().name());
		ndb.setLogNumber(0);
		ndb.setNextFile(2);
		ndb.setLastSequence(0);
		
		String manifest = FileName.getDescriptorFileName(dbname, 1);
		FuncOutput<WritableFile> file = new FuncOutput<>();
		Status s = env.newWritableFile(manifest, file);
		if (!s.ok()) {
			return s;
		}
		
		{
		    LogWriter logWriter = new LogWriter(file.getValue());
		    ByteBuf record = null;
		    ndb.encodeTo(record);
		    s = logWriter.addRecord(new Slice(record)); //TODO
		    if (s.ok()) {
		    	s = file.getValue().close();
		    }
		}
		file.setValue(null);
		
		if (s.ok()) {
			// Make "CURRENT" file that points to the new manifest file.
		    s = FileName.setCurrentFile(env, dbname, 1);
		} else {
		    env.deleteFile(manifest);
		}
		
		return s;
	}
	
	/**
	 * Recover the descriptor from persistent storage.  May do a significant
  	 * amount of work to recover recently logged updates.  Any changes to
  	 * be made to the descriptor are added to edit.</br></br>
  	 * 
  	 * EXCLUSIVE_LOCKS_REQUIRED(mutex);
	 * @param edit
	 * @param saveManifest
	 * @return
	 */
	Status recover(VersionEdit edit, FuncOutputBoolean saveManifest) {
		mutex.assertHeld();
		
		  // Ignore error from CreateDir since the creation of the DB is
		  // committed only when the descriptor is created, and this directory
		  // may already exist from a previous failed creation attempt.
		env.createDir(dbname);
		assert(dbLock == null);
		FuncOutput<FileLock0> dbLockOut = new FuncOutput<>();
		
		Status s = env.lockFile(FileName.getLockFileName(dbname), dbLockOut);
		if (!s.ok()) {
		    return s;
		}
		
		if (!env.fileExists(FileName.getCurrentFileName(dbname))) {
			if (options.createIfMissing) {
				s = newDB();
				if (!s.ok()) {
			        return s;
			    }
			} else {
			      return Status.invalidArgument(dbname + " does not exist (create_if_missing is false)");
			}
		} else {
			if (options.errorIfExists) {
			      return Status.invalidArgument(dbname + " exists (error_if_exists is true)");
			}
		}
		
		s = versions.recover(saveManifest);
		if (!s.ok()) {
			return s;
		}
		FuncOutputLong maxSequence = new FuncOutputLong();
		maxSequence.setValue(0);
		
		  // Recover from all newer log files than the ones named in the
		  // descriptor (new log files may have been added by the previous
		  // incarnation without registering them in the descriptor).
		  //
		  // Note that PrevLogNumber() is no longer used, but we pay
		  // attention to it in case we are recovering a database
		  // produced by an older version of leveldb.
		long minLog = versions.logNumber();
		long prevLog = versions.prevLogNumber();
		ArrayList<String> filenames = new ArrayList<>();
		s = env.getChildren(dbname, filenames);
		if (!s.ok()) {
			return s;
		}
		TreeSet<Long> expected = new TreeSet<Long>();
		versions.addLiveFiles(expected);
		FuncOutputLong number = new FuncOutputLong();
		FuncOutput<FileType> type = new FuncOutput<>();
		ArrayList<Long> logs = new ArrayList<Long>();
		for (int i = 0; i < filenames.size(); i++) {
		    if (FileName.parseFileName(filenames.get(i), number, type)) {
		    	expected.remove(number.getValue());
		    	if (type.getValue() == FileType.LogFile && ((number.getValue() >= minLog) || (number.getValue() == prevLog)))
		    		logs.add(number.getValue());
		  	}
		}
		if (!expected.isEmpty()) {
			String errorMsg = String.format("%d missing files; e.g.", expected.size());
		    return Status.corruption(errorMsg+" "+FileName.getTableFileName(dbname, expected.first()));
		}
		  
		// Recover in the order in which the logs were generated
		Collections.sort(logs);
		for (int i = 0; i < logs.size(); i++) {
			s = recoverLogFile(logs.get(i), (i == logs.size() - 1), saveManifest, edit, maxSequence);
			if (!s.ok()) {
				return s;
			}

		    // The previous incarnation may not have written any MANIFEST
		    // records after allocating this log number.  So we manually
		    // update the file number allocation counter in VersionSet.
			versions.markFileNumberUsed(logs.get(i));
		}
		  
		if (versions.lastSequence() < maxSequence.getValue()) {
			versions.setLastSequence(maxSequence.getValue());
		}

		return Status.ok0();
	}
	
	Status maybeIgnoreError(Status s) {
		  if (s.ok() || options.paranoidChecks) {
			    // No change needed
			  return s;
		  } else {
//			  Log(options_.info_log, "Ignoring error %s", s->ToString().c_str());
			  return Status.defaultStatus();
		  }
	}
	
	/**
	 * Delete any unneeded files and stale in-memory entries.
	 */
	void deleteObsoleteFiles() {
		if (!bgError.ok()) {
		    // After a background error, we don't know whether a new version may
		    // or may not have been committed, so we cannot safely garbage collect.
			return;
		}
		
		// Make a set of all of the live files
		TreeSet<Long> live = pendingOutputs;
		versions.addLiveFiles(live);
		
		ArrayList<String> filenames = new ArrayList<>();
		env.getChildren(dbname, filenames); // Ignoring errors on purpose
		FuncOutputLong number = new FuncOutputLong();
		FuncOutput<FileType> type = new FuncOutput<>();
		
		for (int i = 0; i < filenames.size(); i++) {
		    if (FileName.parseFileName(filenames.get(i), number, type)) {
		    	boolean keep = true;
		    	switch (type.getValue()) {
		        case LogFile:
		        	keep = ((number.getValue() >= versions.logNumber()) ||
		                  (number.getValue() == versions.prevLogNumber()));
		        	break;
		        case DescriptorFile:
		          // Keep my manifest file, and any newer incarnations'
		          // (in case there is a race that allows other incarnations)
		        	keep = (number.getValue() >= versions.manifestFileNumber());
		        	break;
		        case TableFile:
		        	keep = (live.contains(number.getValue()));
		        	break;
		        case TempFile:
		          // Any temp files that are currently being written to must
		          // be recorded in pending_outputs_, which is inserted into "live"
		        	keep = (live.contains(number.getValue()));
		        	break;
		        case CurrentFile:
		        case DBLockFile:
		        case InfoLogFile:
		        	keep = true;
		        	break;
		      }

		      if (!keep) {
		    	  if (type.getValue() == FileType.TableFile) {
		    		  tcache.evict(number.getValue());
		    	  }
//		        Log(options_.info_log, "Delete type=%d #%lld\n",
//		            int(type),
//		            static_cast<unsigned long long>(number));
		    	  env.deleteFile(dbname + "/" + filenames.get(i));
		      }
		   }
		}
	}
	
	/**
	 * Compact the in-memory write buffer to disk.  Switches to a new
     * log-file/memtable and writes a new descriptor iff successful.
     * Errors are recorded in {@code bgError}.</br></br>
     * 
     * EXCLUSIVE_LOCKS_REQUIRED(mutex)
	 */
	void compactMemTable() {
		mutex.assertHeld();
		assert(immtable != null);
		
		// Save the contents of the memtable as a new Table
		VersionEdit edit = new VersionEdit();
		Version base = versions.current(); //base->Ref();
		Status s = writeLevel0Table(immtable, edit, base);
		base = null; //base->Unref();
		
		if (s.ok() && shuttingDown.get() != null) { //shutting_down_.Acquire_Load()
		    s = new Status(Status.Code.IOError, "Deleting DB during memtable compaction");
		}
		
		// Replace immutable memtable with the generated Table
		if (s.ok()) {
			edit.setPrevLogNumber(0);
			edit.setLogNumber(logFileNumber);  // Earlier logs no longer needed
			s = versions.logAndApply(edit, mutex);
		}
		
		if (s.ok()) {
		    // Commit to the new state
		    immtable = null; //imm_->Unref();
		    hasImm.set(null); //has_imm_.Release_Store(NULL);
		    deleteObsoleteFiles();
		} else {
			recordBackgroundError(s);
		}
	}
	
	static class LogReporter implements LogReader.Reporter {
		Env env;
	    Logger0 infoLog;
	    String fname;
	    Status status;  // NULL if options_.paranoid_checks==false
	    
		public void corruption(int bytes, Status s) {
//		      Log(info_log, "%s%s: dropping %d bytes; %s",
//		              (this->status == NULL ? "(ignoring error) " : ""),
//		              fname, static_cast<int>(bytes), s.ToString().c_str());
			if (status != null && status.ok()) 
				status = s.clone();
		}
	}
	/**
	 * EXCLUSIVE_LOCKS_REQUIRED(mutex)
	 * 
	 * @param logNumber
	 * @param lastLog
	 * @param saveManifest
	 * @param edit
	 * @param maxSequence
	 * @return
	 */
	Status recoverLogFile(final long logNumber, boolean lastLog, FuncOutputBoolean saveManifest,
            VersionEdit edit, FuncOutputLong maxSequence) {
		mutex.assertHeld();
		
		// Open the log file
		String fname = FileName.getLogFileName(dbname, logNumber);
		FuncOutput<SequentialFile> fileFuncOut = new FuncOutput<>();
		Status status = env.newSequentialFile(fname, fileFuncOut);
		SequentialFile file = fileFuncOut.getValue();
		if (!status.ok()) {
			status = maybeIgnoreError(status);
		    return status;
		}
		
		// Create the log reader.
		LogReporter reporter = new LogReporter();
		reporter.env = env;
		reporter.infoLog = options.infoLog;
		reporter.fname = fname; //reporter.fname = fname.c_str();
		reporter.status = (options.paranoidChecks ? status : null);
		
		// We intentionally make log::Reader do checksumming even if
		// paranoid_checks==false so that corruptions cause entire commits
		// to be skipped instead of propagating bad information (like overly
		// large sequence numbers).
		LogReader reader = new LogReader(file, reporter, true, 0);
//		  Log(options_.info_log, "Recovering log #%llu",
//			      (unsigned long long) log_number);
		
		// Read all the records and add to a memtable
		ByteBuf scratch = null; //std::string scratch
		Slice record = new Slice();
		WriteBatch batch = new WriteBatch();
		int compactions = 0;
		MemTable mem = null;
		while (reader.readRecord(record, scratch) && status.ok()) {
			if (record.size() < 12) {
			      reporter.corruption(record.size(), Status.corruption("log record too small"));
			      continue;
			}
			
			WriteBatchInternal.setContents(batch, record);
			
			if (mem == null) {
				mem = new MemTable(internalComparator);
			    //mem0->Ref();
			}
			 
			status = WriteBatchInternal.insertInto(batch, mem);
			status = maybeIgnoreError(status);
		    if (!status.ok()) {
		      break;
		    }
		    
		    final long lastSeq  = 
		    		WriteBatchInternal.sequence(batch) +
		            WriteBatchInternal.count(batch) - 1;
		    
		    if (lastSeq > maxSequence.getValue()) {
		        maxSequence.setValue(lastSeq);
		    }
		    
		    if (mem.approximateMemoryUsage() > options.writeBufferSize) {
		    	compactions++;
		    	saveManifest.setValue(true);
		    	status = writeLevel0Table(mem, edit, null);
		    	mem = null; //mem->Unref();
		    	if (!status.ok()) {
		            // Reflect errors immediately so that conditions like full
		            // file-systems cause the DB::Open() to fail.
		            break;
		        }
		    }
		}
		
		//TODO: should close file
		file = null;
		
		// See if we should keep reusing the last log file.
		if (status.ok() && options.reuseLogs && lastLog && compactions == 0) {
			assert(logFile == null);
		    assert(logWriter == null);
		    assert(memtable == null);
		    
		    FuncOutputLong lfileSize = new FuncOutputLong();
		    FuncOutput<WritableFile> logFile0 = new FuncOutput<>();
		    
		    if (env.getFileSize(fname, lfileSize).ok() && env.newAppendableFile(fname, logFile0).ok()) {
		    	logFile = logFile0.getValue();
//		    	Log(options_.info_log, "Reusing old log %s \n", fname.c_str());
		    	logWriter = new LogWriter(logFile, lfileSize.getValue());
		    	logFileNumber = logNumber;
		    	if (mem != null) {
		            memtable = mem;
		            mem = null;
		        } else {
		            // mem can be NULL if lognum exists but was empty.
		            memtable = new MemTable(internalComparator);
		            //mem_->Ref();
		        }
		    }
		}
		
		if (mem != null) {
			// mem did not get reused; compact it.
			if (status.ok()) {
				saveManifest.setValue(true);
				status = writeLevel0Table(mem, edit, null);
			}
			mem = null; //mem->Unref();
		}
		
		return status;
	}
	
	/**
	 * EXCLUSIVE_LOCKS_REQUIRED(mutex)
	 * 
	 * @param mem
	 * @param edit
	 * @param base
	 * @return
	 */
	Status writeLevel0Table(MemTable mem, VersionEdit edit, Version base) {
		mutex.assertHeld();
		long startMillis = env.nowMillis();
		FileMetaData meta = new FileMetaData();
		meta.number = versions.newFileNumber();
		pendingOutputs.add(meta.number);
		Iterator0 iter = mem.newIterator();
//		Log(options_.info_log, "Level-0 table #%llu: started",
//			      (unsigned long long) meta.number);
		
		Status s = null;
		{
		    mutex.unlock();
		    try {
		    	s = Builder.buildTable(dbname, env, options, tcache, iter, meta);
		    } catch (Exception e) {
		    	//TODO
		    }
		    mutex.lock();
		}
		
//		  Log(options_.info_log, "Level-0 table #%llu: %lld bytes %s",
//			      (unsigned long long) meta.number,
//			      (unsigned long long) meta.file_size,
//			      s.ToString().c_str());
		iter = null;
		
		pendingOutputs.remove(meta.number);
		
		// Note that if file_size is zero, the file has been deleted and
		// should not be added to the manifest.
		int level = 0;
		if (s.ok() && meta.fileSize > 0) {
			Slice minUserKey = meta.smallest.userKey();
			Slice maxUserKey = meta.largest.userKey();
			if (base != null) {
				level = base.pickLevelForMemTableOutput(minUserKey, maxUserKey);
			}
			edit.addFile(level, meta.number, meta.fileSize, meta.smallest, meta.largest);
		}
		
		CompactionStats stat = new CompactionStats();
		stat.millis = env.nowMillis() - startMillis;
		stat.bytesWritten = meta.fileSize;
		stats[level].add(stat);
		
		return s;
	}
	
	/**
	 * EXCLUSIVE_LOCKS_REQUIRED(mutex)
	 * 
	 * @param force
	 * @return
	 */
	Status makeRoomForWrite(boolean force /* compact even if there is room? */) {
		//TODO
		mutex.assertHeld();
		assert(!writers.isEmpty());
		try {
			boolean allowDelay = !force;
			Status s = null;
			while (true) {
				if (!bgError.ok()) {// Yield previous error
				      s = bgError.clone();
				      break;
				} else if (allowDelay &&
				        versions.numLevelFiles(0) >= DBFormat.kL0_SlowdownWritesTrigger) {
				      // We are getting close to hitting a hard limit on the number of
				      // L0 files.  Rather than delaying a single write by several
				      // seconds when we hit the hard limit, start delaying each
				      // individual write by 1ms to reduce latency variance.  Also,
				      // this delay hands over some CPU to the compaction thread in
				      // case it is sharing the same core as the writer.
				      mutex.unlock();
				      env.sleepForMilliseconds(1);
				      allowDelay = false;  // Do not delay a single write more than once
				      mutex.lock();
				 } else if (!force &&
				               (memtable.approximateMemoryUsage() <= options.writeBufferSize)) {
				      // There is room in current memtable
				      break;
				 } else if (immtable != null) {
				      // We have filled up the current memtable, but the previous
				      // one is still being compacted, so we wait.
	//			      Log(options_.info_log, "Current memtable full; waiting...\n");
				      bgCv.await();
				 } else if (versions.numLevelFiles(0) >= DBFormat.kL0_StopWritesTrigger) {
				      // There are too many level-0 files.
	//			      Log(options_.info_log, "Too many L0 files; waiting...\n");
					 bgCv.await();
				 } else {
				      // Attempt to switch to a new memtable and trigger compaction of old
				      assert(versions.prevLogNumber() == 0);
				      long newLogNumber = versions.newFileNumber();
				      FuncOutput<WritableFile> lfile = new FuncOutput<>();
				      s = env.newWritableFile(FileName.getLogFileName(dbname, newLogNumber), lfile);
				      if (!s.ok()) {
				    	  // Avoid chewing through file number space in a tight loop.
				    	  versions.reuseFileNumber(newLogNumber);
				    	  break;
				      }
				      logWriter = null; // TODO: delete log_;
				      logFile = null; //TODO: delete logfile_;
				      logFile = lfile.getValue();
				      logFileNumber = newLogNumber;
				      logWriter = new LogWriter(lfile.getValue());
				      immtable = memtable;
				      hasImm.set(immtable); //has_imm_.Release_Store(imm_);
				      memtable = new MemTable(internalComparator);
				      //mem_->Ref();
				      force = false;   // Do not force another compaction if have room
				      maybeScheduleCompaction();
				 }
			}
			return s; //TODO: May be null
		} catch (Exception e) {
			//TODO:
			return null;
		}
	}
	
	WriteBatch buildBatchGroup(FuncOutput<Writer> lastWriterOut) {
		assert(!writers.isEmpty());
		Writer first = writers.peekFirst();
		WriteBatch result = first.batch;
		assert(result != null);

		long size = WriteBatchInternal.byteSize(first.batch);
		 
		// Allow the group to grow up to a maximum size, but if the
		  // original write is small, limit the growth so we do not slow
		  // down the small write too much.
		long maxSize = 1 << 20;
		if (size <= (128<<10)) {
			maxSize = size + (128<<10);
		}
		
		lastWriterOut.setValue(first);
		Iterator<Writer> iter = writers.iterator();
		iter.next(); // Advance past "first"
		while (iter.hasNext()) {
			Writer w = iter.next();
			if (w.sync && !first.sync) {
				// Do not include a sync write into a batch handled by a non-sync write.
				break;
			}
			if (w.batch != null) {
				size += WriteBatchInternal.byteSize(w.batch);
			    if (size > maxSize) {
			    	// Do not make batch too big
			        break;
			    }

			     // Append to *result
			     if (result == first.batch) {
			    	 // Switch to temporary batch instead of disturbing caller's batch
			    	 result = tmpBatch;
			         assert(WriteBatchInternal.count(result) == 0);
			         WriteBatchInternal.append(result, first.batch);
			     }
			     WriteBatchInternal.append(result, w.batch);
			}
			lastWriterOut.setValue(w);
		}
		return result;
	}
	
	void recordBackgroundError(Status s) {
		mutex.assertHeld();
		if (bgError.ok()) {
			bgError = s;
			bgCv.signalAll();
		}
	}
	
	/**
	 * EXCLUSIVE_LOCKS_REQUIRED(mutex)
	 */
	void maybeScheduleCompaction() {
		mutex.assertHeld();
		if (bgCompactionScheduled) {
		    // Already scheduled
		} else if (shuttingDown.get() != null) { //shutting_down_.Acquire_Load()
		    // DB is being deleted; no more background compactions
		} else if (!bgError.ok()) {
		    // Already got an error; no more changes
		} else if (immtable == null &&
		             manualCompaction == null &&
		             !versions.needsCompaction()) {
		    // No work to be done
		} else {
			bgCompactionScheduled = true;
		    //TODO: env.schedule(&DBImpl::BGWork, this);
		}
	}
	
	static void bgWork(Object obj) {
		DBImpl db = (DBImpl)obj;
		db.backgroundCall();
	}
	
	void backgroundCall() {
		mutex.lock();
		try {
			assert(bgCompactionScheduled);
			if (shuttingDown.get() != null) { //shutting_down_.Acquire_Load()
			    // No more background work when shutting down.
			} else if (!bgError.ok()) {
			    // No more background work after a background error.
			} else {
			    backgroundCompaction();
			}
			
			bgCompactionScheduled = false;
			
			// Previous compaction may have produced too many files in a level,
			// so reschedule another compaction if needed.
			maybeScheduleCompaction();
			bgCv.signalAll();
		} finally {
			mutex.unlock();
		}
	}
	
	/**
	 * EXCLUSIVE_LOCKS_REQUIRED(mutex)
	 */
	void  backgroundCompaction(){
		mutex.assertHeld();
		if (immtable != null) {
			compactMemTable();
			return;
		}
		
		Compaction c;
		boolean isManual = (manualCompaction != null);
		InternalKey manualEnd = null;
		if (isManual) {
			ManualCompaction m = manualCompaction;
			c = versions.compactRange(m.level, m.begin, m.end);
			m.done = (c == null);
			if (c != null) {
				manualEnd = c.input(0, c.numInputFiles(0) - 1).largest;
			}
//			    Log(options_.info_log,
//			        "Manual compaction at level-%d from %s .. %s; will stop at %s\n",
//			        m->level,
//			        (m->begin ? m->begin->DebugString().c_str() : "(begin)"),
//			        (m->end ? m->end->DebugString().c_str() : "(end)"),
//			        (m->done ? "(end)" : manual_end.DebugString().c_str()));
		} else {
			c = versions.pickCompaction();
		}
		
	    Status status = Status.defaultStatus();
	    if (c == null) {
	    	// Nothing to do
	    } else if (!isManual && c.isTrivialMove()) {
	    	// Move file to next level
	    	assert(c.numInputFiles(0) == 1);
	    	FileMetaData f = c.input(0, 0);
	    	c.edit().deleteFile(c.level(), f.number);
	    	c.edit().addFile(c.level() + 1, f.number, f.fileSize,
                     f.smallest, f.largest);
	    	status = versions.logAndApply(c.edit(), mutex);
	    	if (!status.ok()) {
	    		recordBackgroundError(status);
	    	}
//	    	VersionSet.LevelSummaryStorage tmp;
//	        Log(options_.info_log, "Moved #%lld to level-%d %lld bytes %s: %s\n",
//	                static_cast<unsigned long long>(f->number),
//	                c->level() + 1,
//	                static_cast<unsigned long long>(f->file_size),
//	                status.ToString().c_str(),
//	                versions_->LevelSummary(&tmp));
	    } else {
	    	CompactionState compact = new CompactionState(c);
	    	try {
	    		status = doCompactionWork(compact);
	    	} catch (Exception e) {
	    		//TODO
	    	}
	        if (!status.ok()) {
	          recordBackgroundError(status);
	        }
	        cleanupCompaction(compact);
	        c.releaseInputs();
	        deleteObsoleteFiles();
	    }
	    
	    c = null;
	    
	    if (status.ok()) {
	        // Done
	    } else if (shuttingDown.get() != null) { //shutting_down_.Acquire_Load()
	        // Ignore compaction errors found during shutting down
	    } else {
//	        Log(options_.info_log,
//	            "Compaction error: %s", status.ToString().c_str());
	    }
	    
	    if (isManual) {
	        ManualCompaction m = manualCompaction;
	        if (!status.ok()) {
	        	m.done = true;
	        }
	        if (!m.done) {
	          // We only compacted part of the requested range.  Update *m
	          // to the range that is left to be compacted.
	        	m.tmpStorage = manualEnd;
	        	m.begin = m.tmpStorage;
	        }
	        manualCompaction = null;
	    }
	}
	
	/**
	 * EXCLUSIVE_LOCKS_REQUIRED(mutex)
	 * @param compact
	 */
	void cleanupCompaction(CompactionState compact) {
		mutex.assertHeld();
		if (compact.builder !=null) {
			// May happen if we get a shutdown call in the middle of compaction
		    compact.builder.abandon();
		    compact.builder = null;
		} else {
			assert(compact.outFile == null);
		}
		compact.outFile = null;
		for (int i = 0; i < compact.outputs.size(); i++) {
		    CompactionState.Output out = compact.outputs.get(i);
		    pendingOutputs.remove(out.number);
		}
		compact = null;
	}
	
	// Make the output file
	Status openCompactionOutputFile(CompactionState compact) {
		assert(compact != null);
		assert(compact.builder == null);
		
		long fileNumber;
		{
			mutex.lock();
			try {
				fileNumber = versions.newFileNumber();
			    pendingOutputs.add(fileNumber);
			    CompactionState.Output out = new CompactionState.Output();
			    out.number = fileNumber;
			    out.smallest.clear();
			    out.largest.clear();
			    compact.outputs.add(out);
			} finally {
				mutex.unlock();
			}
		}
		
		// Make the output file
		String fname = FileName.getTableFileName(dbname, fileNumber);
		FuncOutput<WritableFile> resultOutFile = new FuncOutput<WritableFile>();
		Status s = env.newWritableFile(fname, resultOutFile);
		compact.outFile = resultOutFile.getValue();
		if (s.ok())
		    compact.builder = new TableBuilder(options, compact.outFile);
		    
		return s;
	}
	
	Status finishCompactionOutputFile(CompactionState compact, Iterator0 input) {
		assert(compact != null);
		assert(compact.outFile != null);
		assert(compact.builder != null);
		
		final long outputNumber = compact.currentOutput().number;
		assert(outputNumber != 0);
		
		Status s = input.status();
		final long currentEntries = compact.builder.numEntries();
		if (s.ok()) {
			s = compact.builder.finish();
		} else {
			compact.builder.abandon();
		}
		final long currentBytes = compact.builder.fileSize();
		compact.currentOutput().fileSize = currentBytes;
		compact.totalBytes += currentBytes;
		compact.builder = null;
		
		// Finish and check for file errors
		if (s.ok()) {
		    s = compact.outFile.sync();
		}
		if (s.ok()) {
		    s = compact.outFile.close();
		}
		compact.outFile = null;
		
		if (s.ok() && currentEntries > 0) {
			// Verify that the table is usable
			Iterator0 iter = tcache.newIterator(new ReadOptions(),
                    outputNumber,
                    currentBytes);
			
			s = iter.status();
			iter = null;
			if (s.ok()) {
//				Log(options_.info_log,
//				          "Generated table #%llu@%d: %lld keys, %lld bytes",
//				          (unsigned long long) output_number,
//				          compact->compaction->level(),
//				          (unsigned long long) current_entries,
//				          (unsigned long long) current_bytes);
			}
		}
		
		return s;
	}
	
	/**
	 * EXCLUSIVE_LOCKS_REQUIRED(mutex)
	 * @param compact
	 * @return
	 */
	Status installCompactionResults(CompactionState compact) {
		//TODO
		mutex.assertHeld();
//		  Log(options_.info_log,  "Compacted %d@%d + %d@%d files => %lld bytes",
//			      compact->compaction->num_input_files(0),
//			      compact->compaction->level(),
//			      compact->compaction->num_input_files(1),
//			      compact->compaction->level() + 1,
//			      static_cast<long long>(compact->total_bytes));
		compact.compaction.addInputDeletions(compact.compaction.edit());
		int level = compact.compaction.level();
		for (int i = 0; i < compact.outputs.size(); i++) {
			CompactionState.Output out = compact.outputs.get(i);
			compact.compaction.edit().addFile(level+1, out.number, out.fileSize, out.smallest, out.largest);
		}
		return versions.logAndApply(compact.compaction.edit(), mutex);
	}
	
	/*
	 * EXCLUSIVE_LOCKS_REQUIRED(mutex)
	 */
	Status doCompactionWork(CompactionState compact) throws Exception {
		//TODO
		long startMillis = env.nowMillis();
		long immMillis = 0;  // millis spent doing imm compactions
		
//	  Log(options_.info_log,  "Compacting %d@%d + %d@%d files",
//		      compact->compaction->num_input_files(0),
//		      compact->compaction->level(),
//		      compact->compaction->num_input_files(1),
//		      compact->compaction->level() + 1);
		
		assert(versions.numLevelFiles(compact.compaction.level()) > 0);
		assert(compact.builder == null);
		assert(compact.outFile == null);
		if (snapshots.isEmpty()) {
			compact.smallestSnapshot = versions.lastSequence;
		} else {
			compact.smallestSnapshot = snapshots.oldest().number;
		}
		
		// Release mutex while we're actually doing the compaction work
		mutex.unlock();
		
		
		Iterator0 input = versions.makeInputIterator(compact.compaction);
		input.seekToFirst();
		Status status = Status.defaultStatus();
		ParsedInternalKey ikey = new ParsedInternalKey();
		ByteBuf currentUserKey = ByteBufFactory.defaultByteBuf();
		boolean hasCurrentUserKey = false;
		long lastSequenceForKey = DBFormat.kMaxSequenceNumber;
		for (; input.valid() && shuttingDown.get() != null; ) { //shutting_down_.Acquire_Load()
			// Prioritize immutable compaction work
			if (hasImm.get() != null) { //has_imm_.NoBarrier_Load() != NULL
				long immStart = env.nowMillis();
				mutex.lock();
				if (immtable != null) {
					compactMemTable();
					bgCv.signalAll();
				}
				mutex.unlock();
				immMillis += (env.nowMillis() - immStart);
			}
			
			Slice key = input.key();
			if (compact.compaction.shouldStopBefore(key) && 
					compact.builder != null) {
				status = finishCompactionOutputFile(compact, input);
				if (!status.ok())
					break;
			}
			
			// Handle key/value, add to state, etc.
			boolean drop = false;
			if (!ikey.parse(key)) {
				// Do not hide error keys
				currentUserKey.clear();
				hasCurrentUserKey = false;
				lastSequenceForKey = DBFormat.kMaxSequenceNumber;
			} else {
				if (!hasCurrentUserKey || userComparator().compare(ikey.userKey, new Slice(currentUserKey)) != 0) {
					// First occurrence of this user key
					currentUserKey.assign(ikey.userKey.data(), ikey.userKey.offset(), ikey.userKey.size());
					hasCurrentUserKey = true;
					lastSequenceForKey = DBFormat.kMaxSequenceNumber;
				}
				
				if (lastSequenceForKey <= compact.smallestSnapshot) {
					// Hidden by an newer entry for same user key
					drop = true;
				} else if (ikey.type == ValueType.Deletion && 
						ikey.sequence <= compact.smallestSnapshot &&
						compact.compaction.isBaseLevelForKey(ikey.userKey)){
			        // For this user key:
			        // (1) there is no data in higher levels
			        // (2) data in lower levels will have larger sequence numbers
			        // (3) data in layers that are being compacted here and have
			        //     smaller sequence numbers will be dropped in the next
			        //     few iterations of this loop (by rule (A) above).
			        // Therefore this deletion marker is obsolete and can be dropped.
					drop = true;
				}
				
				lastSequenceForKey = ikey.sequence;
			}
			
			if (drop) {
				 // Open output file if necessary
				if (compact.builder == null) {
					status = this.openCompactionOutputFile(compact);
					if (!status.ok())
						break;
				}
				
				if (compact.builder.numEntries() == 0) { //TODO: BUGS?
					compact.currentOutput().smallest.decodeFrom(key);
				}
				compact.currentOutput().largest.decodeFrom(key);
				compact.builder.add(key, input.value());
				
				// Close output file if it is big enough
				if (compact.builder.fileSize() >= compact.compaction.maxOutputFileSize()) {
					status = finishCompactionOutputFile(compact, input);
					if (!status.ok())
						break;
				}
			}
			
			input.next();
		}
		
		if (status.ok() && shuttingDown.get() != null) {
			status = new Status(Status.Code.IOError, "Deleting DB during compaction");
		}
		if (status.ok() && compact.builder != null) {
			status = finishCompactionOutputFile(compact, input);
		}
		if (status.ok()) {
			status = input.status();
		}
		
		
		CompactionStats stat = new CompactionStats();
		stat.millis = env.nowMillis() - startMillis - immMillis;
		for (int which = 0; which < 2; which++) {
			for (int i = 0; i < compact.compaction.numInputFiles(which); i++) {
				stat.bytesRead += compact.compaction.input(which, i).fileSize;
			}
		}
		for (int i = 0; i < compact.outputs.size(); i++) {
			stat.bytesWritten += compact.outputs.get(i).fileSize;
		}
		
		mutex.lock();
		stats[compact.compaction.level() + 1].add(stat);
		
		if (status.ok()) {
		    status = installCompactionResults(compact);
		}
		
		if (!status.ok()) {
			recordBackgroundError(status);
		}
		
//		  VersionSet::LevelSummaryStorage tmp;
//		  Log(options_.info_log,
//		      "compacted to: %s", versions_->LevelSummary(&tmp));
		return status;
	}
	
	
	Env env;
	InternalKeyComparator internalComparator;
	InternalFilterPolicy internalFilterPolicy;
	Options options;
	boolean ownsInfoLog;
	boolean ownsCache;
	String dbname;
	
	/**
	 * tcache provides its own synchronization
	 */
	TableCache tcache;
	
	/**
	 * Lock over the persistent DB state.  non-null iff successfully acquired.
	 */
	FileLock0 dbLock;
	
	Mutex mutex;
	AtomicReference<Object> shuttingDown;
	CondVar bgCv;	// Signalled when background work finishes
	MemTable memtable;
	MemTable immtable;   // Memtable being compacted
	AtomicReference<Object> hasImm; // So bg thread can detect non-null imm
	WritableFile logFile;
	long logFileNumber;
	LogWriter logWriter;
	int seed;
	
	// Queue of writers.
	Deque<Writer> writers;
	WriteBatch tmpBatch;
	
	SnapshotList snapshots;
	
	/**
	 * Set of table files to protect from deletion because they are
	 * part of ongoing compactions.
	 */
	TreeSet<Long> pendingOutputs;
	
	/**
	 * Has a background compaction been scheduled or is running?
	 */
	boolean bgCompactionScheduled;
	
	class ManualCompaction {
		public int level;
		public boolean done;
		public InternalKey begin;   // null means beginning of key range
		public InternalKey end;     // NULL means end of key range
		public InternalKey tmpStorage;    // Used to keep track of compaction progress
	}
	ManualCompaction manualCompaction;
	
	VersionSet versions;
	
	// Have we encountered a background error in paranoid mode?
	Status bgError;
	
	class CompactionStats {
		public long millis;
		public long bytesRead;
		public long bytesWritten;
		
		void add(CompactionStats c) {
			millis += c.millis;
			bytesRead += c.bytesRead;
			bytesWritten += c.bytesWritten;
		}
	}
	CompactionStats stats[] = new CompactionStats[DBFormat.kNumLevels];
	
	public Comparator0 userComparator() {
		return internalComparator.userComparator();
	}
	
	// Record a sample of bytes read at the specified internal key.
	  // Samples are taken approximately once every config::kReadBytesPeriod
	  // bytes.
	public void recordReadSample(Slice key) {
		//TODO
	}
	
	public Iterator0 newDBIterator(Comparator0 userKeyComparator, Iterator0 internalCursor, long squence, int seed) {
		//TODO
		return null;
	}
}
