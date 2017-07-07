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
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;

import com.tchaicatkovsky.jleveldb.DB;
import com.tchaicatkovsky.jleveldb.Env;
import com.tchaicatkovsky.jleveldb.FileLock0;
import com.tchaicatkovsky.jleveldb.FileName;
import com.tchaicatkovsky.jleveldb.FileType;
import com.tchaicatkovsky.jleveldb.Iterator0;
import com.tchaicatkovsky.jleveldb.Logger0;
import com.tchaicatkovsky.jleveldb.Options;
import com.tchaicatkovsky.jleveldb.Range;
import com.tchaicatkovsky.jleveldb.ReadOptions;
import com.tchaicatkovsky.jleveldb.SequentialFile;
import com.tchaicatkovsky.jleveldb.Snapshot;
import com.tchaicatkovsky.jleveldb.SnapshotList;
import com.tchaicatkovsky.jleveldb.Status;
import com.tchaicatkovsky.jleveldb.WritableFile;
import com.tchaicatkovsky.jleveldb.WriteBatch;
import com.tchaicatkovsky.jleveldb.WriteOptions;
import com.tchaicatkovsky.jleveldb.db.format.DBFormat;
import com.tchaicatkovsky.jleveldb.db.format.InternalFilterPolicy;
import com.tchaicatkovsky.jleveldb.db.format.InternalKey;
import com.tchaicatkovsky.jleveldb.db.format.InternalKeyComparator;
import com.tchaicatkovsky.jleveldb.db.format.LookupKey;
import com.tchaicatkovsky.jleveldb.db.format.ParsedInternalKey;
import com.tchaicatkovsky.jleveldb.db.format.ValueType;
import com.tchaicatkovsky.jleveldb.table.MergingIterator;
import com.tchaicatkovsky.jleveldb.table.TableBuilder;
import com.tchaicatkovsky.jleveldb.util.Boolean0;
import com.tchaicatkovsky.jleveldb.util.ByteBuf;
import com.tchaicatkovsky.jleveldb.util.ByteBufFactory;
import com.tchaicatkovsky.jleveldb.util.Cache;
import com.tchaicatkovsky.jleveldb.util.Comparator0;
import com.tchaicatkovsky.jleveldb.util.CondVar;
import com.tchaicatkovsky.jleveldb.util.Integer0;
import com.tchaicatkovsky.jleveldb.util.Long0;
import com.tchaicatkovsky.jleveldb.util.Mutex;
import com.tchaicatkovsky.jleveldb.util.Object0;
import com.tchaicatkovsky.jleveldb.util.Slice;
import com.tchaicatkovsky.jleveldb.util.SliceFactory;

public class DBImpl implements DB {

	Env env;
	InternalKeyComparator internalComparator;
	InternalFilterPolicy internalFilterPolicy;
	Options options;
	boolean ownsInfoLog;
	boolean ownsCache;
	String dbname;

	/**
	 * tableCache provides its own synchronization
	 */
	TableCache tableCache;

	/**
	 * Lock over the persistent DB state. non-null iff successfully acquired.
	 */
	FileLock0 dbLock = null;

	Mutex mutex;
	AtomicReference<Object> shuttingDown;
	/**
	 * Signalled when background work finishes
	 */
	CondVar bgCv;
	MemTable memtable = null;
	/**
	 * Memtable being compacted
	 */
	MemTable immtable = null;
	AtomicReference<Object> hasImm = new AtomicReference<Object>(); // So bg thread can detect non-null imm
	WritableFile logFile = null;
	long logFileNumber = 0;
	LogWriter logWriter = null;
	int seed = 0;

	// Queue of writers.
	Deque<Writer> writers = new LinkedList<>();
	WriteBatch tmpBatch = new WriteBatch();

	SnapshotList snapshots = new SnapshotList();

	/**
	 * Set of table files to protect from deletion because they are part of ongoing
	 * compactions.
	 */
	TreeSet<Long> pendingOutputs = new TreeSet<>();

	/**
	 * Has a background compaction been scheduled or is running?
	 */
	boolean bgCompactionScheduled;

	class ManualCompaction {
		public int level;
		public boolean done;
		/**
		 * null means beginning of key range
		 */
		public InternalKey begin;
		/**
		 * null means end of key range
		 */
		public InternalKey end;
		/**
		 * Used to keep track of compaction progress
		 */
		public InternalKey tmpStorage = new InternalKey();
	}

	ManualCompaction manualCompaction;

	VersionSet versions;

	/**
	 *  Have we encountered a background error in paranoid mode?
	 */
	Status bgError = Status.ok0();

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

		public Writer(Mutex lock, WriteBatch batch, boolean sync, boolean done) {
			status = Status.ok0();
			this.lock = lock;
			this.batch = batch;
			this.sync = sync;
			this.done = done;
			cv = lock.newCondVar();
		}
	}

	public static class CompactionState {
		final Compaction compaction;

		/**
		 * Sequence numbers < smallest_snapshot are not significant since we will never
		 * have to service a snapshot below smallest_snapshot.</br>
		 * Therefore if we have seen a sequence number S <= smallest_snapshot, we can
		 * drop all entries for the same key with sequence numbers < S.
		 */
		long smallestSnapshot;

		/**
		 * Files produced by compaction
		 */
		public static class Output {
			long number;
			long fileSize;
			InternalKey smallest = new InternalKey();
			InternalKey largest = new InternalKey();
			int numEntries;
		}

		ArrayList<Output> outputs;

		/**
		 * State kept for output being generated
		 */
		WritableFile outFile;
		TableBuilder builder;

		long totalBytes;

		public Output currentOutput() {
			return outputs.get(outputs.size() - 1);
		}

		public CompactionState(Compaction compaction) {
			this.compaction = compaction;
			this.outputs = new ArrayList<Output>();
			this.totalBytes = 0;
			outputs = new ArrayList<>();
		}
	}

	int clipToRange(int src, int minValue, int maxValue) {
		return (int) clipToRange((long) src, (long) minValue, (long) maxValue);
	}

	long clipToRange(long src, long minValue, long maxValue) {
		long ret = src;
		if (ret > maxValue)
			ret = maxValue;
		if (ret < minValue)
			ret = minValue;
		return ret;
	}

	final static int kNumNonTableCacheFiles = 10;

	Options sanitizeOptions(String dbname, InternalKeyComparator icmp, InternalFilterPolicy ipolicy, Options src) {
		Options result = src.cloneOptions();
		result.comparator = icmp;
		result.filterPolicy = (src.filterPolicy != null) ? ipolicy : null;
		result.maxOpenFiles = clipToRange(result.maxOpenFiles, 64 + kNumNonTableCacheFiles, 50000);
		result.writeBufferSize = clipToRange(result.writeBufferSize, 64 << 10, 1 << 30);
		result.maxFileSize = clipToRange(result.maxFileSize, 1 << 20, 1 << 30);
		result.blockSize = clipToRange(result.blockSize, 1 << 10, 4 << 20);

		Object0<Logger0> log0 = new Object0<>();
		if (result.infoLog == null) {
			// Open a log file in the same directory as the db
			src.env.createDir(dbname); // In case it does not exist
			if (src.env.fileExists(FileName.getInfoLogFileName(dbname)))
				src.env.renameFile(FileName.getInfoLogFileName(dbname), FileName.getOldInfoLogFileName(dbname));
			Status s = src.env.newLogger(FileName.getInfoLogFileName(dbname), log0);
			if (!s.ok()) {
				// No place suitable for logging
				result.infoLog = null;
			}
			result.infoLog = log0.getValue();

			if (result.blockCache == null) {
				result.blockCache = Cache.newLRUCache(8 << 20);
			}
		}

		return result;
	}

	void init(Options rawOptions, String dbname) {
		env = rawOptions.env;
		internalComparator = new InternalKeyComparator(rawOptions.comparator);
		internalFilterPolicy = new InternalFilterPolicy(rawOptions.filterPolicy);
		options = sanitizeOptions(dbname, internalComparator, internalFilterPolicy, rawOptions);
		ownsInfoLog = (options.infoLog != rawOptions.infoLog);
		ownsCache = (options.blockCache != rawOptions.blockCache);
		this.dbname = dbname;
		dbLock = null;
		shuttingDown = null;
		mutex = new Mutex();
		bgCv = mutex.newCondVar();
		memtable = null;
		immtable = null;
		logFile = null;
		logFileNumber = 0;
		logWriter = null;
		seed = 0;
		bgCompactionScheduled = false;
		manualCompaction = null;
		shuttingDown = new AtomicReference<Object>();

		hasImm.set(null);

		/**
		 * Reserve ten files or so for other uses and give the rest to TableCache.
		 */
		int tableCacheSize = options.maxOpenFiles - kNumNonTableCacheFiles;

		tableCache = new TableCache(dbname, options, tableCacheSize);

		versions = new VersionSet(dbname, options, tableCache, internalComparator);

		for (int i = 0; i < DBFormat.kNumLevels; i++)
			stats[i] = new CompactionStats();
	}

	@Override
	public Status open(Options rawOptions, String name) {
		init(rawOptions, name);

		mutex.lock();
		VersionEdit edit = new VersionEdit();
		Boolean0 saveManifest = new Boolean0();
		saveManifest.setValue(false);
		Status s = recover(edit, saveManifest);

		if (s.ok() && memtable == null) {
			// Create new log and a corresponding memtable.
			long newLogNumber = versions.newFileNumber();

			Object0<WritableFile> result = new Object0<WritableFile>();
			s = env.newWritableFile(FileName.getLogFileName(dbname, newLogNumber), result);
			if (s.ok()) {
				edit.setLogNumber(newLogNumber);
				logFile = result.getValue();
				logFileNumber = newLogNumber;
				logWriter = new LogWriter(logFile);
				memtable = new MemTable(this.internalComparator);
				memtable.ref();
			}
		}

		if (s.ok() && saveManifest.getValue()) {
			edit.setPrevLogNumber(0); // No older logs needed after recovery.
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
	public void close() {
		// Wait for background work to finish
		mutex.lock();
		try {
			shuttingDown.set(this); // Any non-NULL value is ok
			while (bgCompactionScheduled) {
				bgCv.await();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			mutex.unlock();
		}

		if (dbLock != null) {
			env.unlockFile(dbLock);
			dbLock = null;
		}

		tableCache.delete();

		versions.delete();

		if (memtable != null)
			memtable.unref();
		if (immtable != null)
			immtable.unref();

		tmpBatch = null;

		if (logWriter != null) {
			logWriter.delete();
			logWriter = null;
		}

		if (logFile != null) {
			logFile.close();
			logFile = null;
		}

		if (ownsInfoLog) {
			options.infoLog.delete();
			options.infoLog = null;
		}

		if (ownsCache) {
			options.blockCache.delete();
			options.blockCache = null;
		}
	}

	@Override
	public Status put(WriteOptions options, Slice key, Slice value) throws Exception {
		WriteBatch batch = new WriteBatch();
		batch.put(key, value);
		return write(options, batch);
	}

	@Override
	public Status delete(WriteOptions options, Slice key) throws Exception {
		WriteBatch batch = new WriteBatch();
		batch.delete(key);
		return write(options, batch);
	}

	@Override
	public Status write(WriteOptions options, WriteBatch batch) throws Exception {
		Writer w = new Writer(mutex, batch, options.isSync(), false);

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
			Object0<Writer> lastWriter = new Object0<Writer>();
			lastWriter.setValue(w);
			if (status.ok() && batch != null) {
				// null batch is for compactions
				WriteBatch updates = buildBatchGroup(lastWriter);
				WriteBatchInternal.setSequence(updates, lastSequence + 1);
				lastSequence += WriteBatchInternal.count(updates);

				// Add to log and apply to memtable. We can release the lock
				// during this phase since w is currently responsible for logging
				// and protects against concurrent loggers and concurrent writes
				// into memtable.
				{
					mutex.unlock();
					status = logWriter.addRecord(WriteBatchInternal.contents(updates));

					boolean syncError = false;
					if (status.ok() && options.sync) {
						status = logFile.sync();
						if (!status.ok())
							syncError = true;
					}

					if (status.ok())
						status = WriteBatchInternal.insertInto(updates, memtable);

					mutex.lock();
					if (syncError) {
						// The state of the log file is indeterminate: the log record we
						// just added may or may not show up when the DB is re-opened.
						// So we force the DB into a mode where all future writes fail.
						recordBackgroundError(status);
					}
				}

				if (updates == tmpBatch)
					tmpBatch.clear();

				versions.setLastSequence(lastSequence);
			}

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
			e.printStackTrace();
			return Status.otherError("" + e);
		} finally {
			mutex.unlock();
		}
	}

	@Override
	public Status get(ReadOptions options, Slice key, ByteBuf value) {
		value.clear();
		Object0<Status> s = new Object0<Status>();
		s.setValue(Status.ok0());
		mutex.lock();
		try {
			long snapshotSeqNumber = 0;
			if (options.snapshot != null) {
				snapshotSeqNumber = options.snapshot.number;
			} else {
				snapshotSeqNumber = versions.lastSequence();
			}

			MemTable mem = memtable;
			MemTable imm = immtable;
			Version current = versions.current();
			mem.ref();
			if (imm != null)
				imm.ref();
			current.ref();

			boolean haveStatUpdate = false;
			Version.GetStats stats = new Version.GetStats();
			{
				mutex.unlock();
				// First look in the memtable, then in the immutable memtable (if any).
				LookupKey lkey = new LookupKey(key, snapshotSeqNumber);

				if (mem.get(lkey, value, s)) {
					// Done
				} else if (imm != null && imm.get(lkey, value, s)) {
					// Done
				} else {
					s.setValue(current.get(options, lkey, value, stats));
					haveStatUpdate = true;
				}
				mutex.lock();
			}

			if (haveStatUpdate && current.updateStats(stats)) {
				maybeScheduleCompaction();
			}

			mem.unref();
			mem = null;
			if (imm != null)
				imm.unref();
			imm = null;
			current.unref();

			return s.getValue();
		} catch (Exception e) {
			e.printStackTrace();
			return Status.otherError("" + e);
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
				state.mem.unref();
				state.mem = null; // state->mem->Unref();

				if (state.imm != null)
					state.imm.unref();
				state.imm = null; // if (state->imm != NULL) state->imm->Unref();

				state.version.unref();
				state.version = null;
			} finally {
				state.mutex.unlock();
			}
		}
	}

	Status newDB() {
		VersionEdit ndb = new VersionEdit();
		ndb.setComparatorName(userComparator().name());
		ndb.setLogNumber(0);
		ndb.setNextFile(2);
		ndb.setLastSequence(0);

		String manifest = FileName.getDescriptorFileName(dbname, 1);
		Object0<WritableFile> file = new Object0<>();
		Status s = env.newWritableFile(manifest, file);
		if (!s.ok()) {
			return s;
		}

		{
			LogWriter logWriter = new LogWriter(file.getValue());
			ByteBuf record = ByteBufFactory.newUnpooled();
			ndb.encodeTo(record);
			s = logWriter.addRecord(SliceFactory.newUnpooled(record));
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
	 * Recover the descriptor from persistent storage. May do a significant amount
	 * of work to recover recently logged updates. Any changes to be made to the
	 * descriptor are added to edit.</br>
	 * </br>
	 * 
	 * EXCLUSIVE_LOCKS_REQUIRED(mutex);
	 * 
	 * @param edit
	 * @param saveManifest
	 * @return
	 */
	Status recover(VersionEdit edit, Boolean0 saveManifest) {
		mutex.assertHeld();

		// Ignore error from CreateDir since the creation of the DB is
		// committed only when the descriptor is created, and this directory
		// may already exist from a previous failed creation attempt.
		env.createDir(dbname);
		assert (dbLock == null);
		Object0<FileLock0> dbLockOut = new Object0<>();

		Status s = env.lockFile(FileName.getLockFileName(dbname), dbLockOut);
		if (!s.ok())
			return s;
		dbLock = dbLockOut.getValue();

		if (!env.fileExists(FileName.getCurrentFileName(dbname))) {
			if (options.createIfMissing) {
				s = newDB();
				if (!s.ok())
					return s;
			} else {
				return Status.invalidArgument(dbname + " does not exist (createIfMissing is false)");
			}
		} else {
			if (options.errorIfExists)
				return Status.invalidArgument(dbname + " exists (errorIfExists is true)");
		}

		s = versions.recover(saveManifest);
		if (!s.ok())
			return s;

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
		if (!s.ok())
			return s;

		TreeSet<Long> expected = new TreeSet<Long>();
		versions.addLiveFiles(expected);
		Long0 number = new Long0();
		Object0<FileType> type = new Object0<>();
		ArrayList<Long> logs = new ArrayList<Long>();
		for (int i = 0; i < filenames.size(); i++) {
			if (FileName.parseFileName(filenames.get(i), number, type)) {
				expected.remove(number.getValue());
				if (type.getValue() == FileType.LogFile && 
						((number.getValue() >= minLog) || (number.getValue() == prevLog)))
					logs.add(number.getValue());
			}
		}
		if (!expected.isEmpty()) {
			String errorMsg = String.format("%d missing files; e.g.", expected.size());
			return Status.corruption(errorMsg + " " + FileName.getTableFileName(dbname, expected.first()));
		}

		// Recover in the order in which the logs were generated
		Long0 maxSequence = new Long0(0);
		Collections.sort(logs);
		for (int i = 0; i < logs.size(); i++) {
			s = recoverLogFile(logs.get(i), (i == logs.size() - 1), saveManifest, edit, maxSequence);
			if (!s.ok())
				return s;

			// The previous incarnation may not have written any MANIFEST
			// records after allocating this log number. So we manually
			// update the file number allocation counter in VersionSet.
			versions.markFileNumberUsed(logs.get(i));
		}

		if (versions.lastSequence() < maxSequence.getValue())
			versions.setLastSequence(maxSequence.getValue());

		return Status.ok0();
	}

	Status maybeIgnoreError(Status s) {
		if (s.ok() || options.paranoidChecks) {
			// No change needed
			return s;
		} else {
			Logger0.log0(options.infoLog, "Ignoring error {}", s);
			return Status.ok0();
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
		TreeSet<Long> live = new TreeSet<Long>();
		live.addAll(pendingOutputs);
		versions.addLiveFiles(live);

		ArrayList<String> filenames = new ArrayList<>();
		env.getChildren(dbname, filenames); // Ignoring errors on purpose
		Long0 number = new Long0();
		Object0<FileType> type = new Object0<>();

		for (int i = 0; i < filenames.size(); i++) {
			if (FileName.parseFileName(filenames.get(i), number, type)) {
				boolean keep = true;
				switch (type.getValue()) {
				case LogFile:
					keep = ((number.getValue() >= versions.logNumber()) || (number.getValue() == versions.prevLogNumber()));
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
						tableCache.evict(number.getValue());
					}

					Logger0.log0(options.infoLog, "Delete type={} #{}\n", type.getValue().name(), number.getValue());

					env.deleteFile(dbname + "/" + filenames.get(i));
				}
			}
		}
	}


	/**
	 * Compact the in-memory write buffer to disk. Switches to a new
	 * log-file/memtable and writes a new descriptor iff successful. Errors are
	 * recorded in {@code bgError}.</br>
	 * </br>
	 * 
	 * EXCLUSIVE_LOCKS_REQUIRED(mutex)
	 */
	void compactMemTable() {
		mutex.assertHeld();
		assert (immtable != null);

		// Save the contents of the memtable as a new Table
		VersionEdit edit = new VersionEdit();
		Version base = versions.current();

		base.ref();
		Status s = writeLevel0Table(immtable, edit, base);
		base.unref();
		base = null;

		if (s.ok() && shuttingDown.get() != null)
			s = new Status(Status.Code.IOError, "Deleting DB during memtable compaction");

		// Replace immutable memtable with the generated Table
		if (s.ok()) {
			edit.setPrevLogNumber(0);
			edit.setLogNumber(logFileNumber); // Earlier logs no longer needed
			s = versions.logAndApply(edit, mutex);
		}

		if (s.ok()) {
			// Commit to the new state
			immtable.unref();
			immtable = null;
			hasImm.set(null);
			deleteObsoleteFiles();
		} else {
			recordBackgroundError(s);
		}
	}

	static class LogReporter implements LogReader.Reporter {
		Env env;
		Logger0 infoLog;
		String fname;
		Status status = Status.ok0();

		public void corruption(int bytes, Status s) {
			Logger0.log0(infoLog, "{}{}: dropping {} bytes; {}", (status == null ? "(ignoring error) " : ""), fname, bytes, s);

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
	Status recoverLogFile(final long logNumber, boolean lastLog, Boolean0 saveManifest, VersionEdit edit, Long0 maxSequence) {
		mutex.assertHeld();

		// Open the log file
		String fname = FileName.getLogFileName(dbname, logNumber);

		Object0<SequentialFile> fileFuncOut = new Object0<>();
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
		reporter.fname = fname; // reporter.fname = fname.c_str();
		reporter.status = (options.paranoidChecks ? status : null);

		// We intentionally make LogReader do checksumming even if
		// paranoid_checks==false so that corruptions cause entire commits
		// to be skipped instead of propagating bad information (like overly
		// large sequence numbers).
		LogReader reader = new LogReader(file, reporter, true, 0);

		Logger0.log0(options.infoLog, "Recovering log #{}", logNumber);

		// Read all the records and add to a memtable
		ByteBuf scratch = ByteBufFactory.newUnpooled(); // std::string scratch
		Slice record = SliceFactory.newUnpooled();
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
				mem.ref();
			}

			status = WriteBatchInternal.insertInto(batch, mem);
			status = maybeIgnoreError(status);
			if (!status.ok()) {
				break;
			}

			final long lastSeq = WriteBatchInternal.sequence(batch) + WriteBatchInternal.count(batch) - 1;

			if (lastSeq > maxSequence.getValue()) {
				maxSequence.setValue(lastSeq);
			}

			if (mem.approximateMemoryUsage() > options.writeBufferSize) {
				compactions++;
				saveManifest.setValue(true);
				status = writeLevel0Table(mem, edit, null);
				mem.unref();
				mem = null;
				if (!status.ok()) {
					// Reflect errors immediately so that conditions like full
					// file-systems cause the DB::Open() to fail.
					break;
				}
			}
		}

		file.delete(); // should close file

		// See if we should keep reusing the last log file.
		if (status.ok() && options.reuseLogs && lastLog && compactions == 0) {
			assert (logFile == null);
			assert (logWriter == null);
			assert (memtable == null);

			Long0 lfileSize = new Long0();
			Object0<WritableFile> logFile0 = new Object0<>();

			Status b1 = env.getFileSize(fname, lfileSize);
			Status b2 = env.newAppendableFile(fname, logFile0);

			if (b1.ok() && b2.ok()) {
				logFile = logFile0.getValue();
				Logger0.log0(options.infoLog, "Reusing old log {} \n", fname);
				logWriter = new LogWriter(logFile, lfileSize.getValue());
				logFileNumber = logNumber;
				if (mem != null) {
					memtable = mem;
					mem = null;
				} else {
					// mem can be null if lognum exists but was empty.
					memtable = new MemTable(internalComparator);
					memtable.ref();
				}
			}
		}

		if (mem != null) {
			// mem did not get reused; compact it.
			if (status.ok()) {
				saveManifest.setValue(true);
				status = writeLevel0Table(mem, edit, null);
			}
			mem.unref();
			mem = null;
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
		Iterator0 memiter = mem.newIterator();
		Logger0.log0(options.infoLog, "Level-0 table #{}: started", meta.number);

		Status s = Status.ok0();
		{
			mutex.unlock();
			try {
				s = Builder.buildTable(dbname, env, options, tableCache, memiter, meta);
			} catch (Exception e) {
				e.printStackTrace();
				throw e;
			}

			mutex.lock();
		}

		memiter.delete();
		memiter = null;

		pendingOutputs.remove(meta.number);

		// Note that if fileSize is zero, the file has been deleted and
		// should not be added to the manifest.
		int level = 0;
		if (s.ok() && meta.fileSize > 0) {
			Slice minUserKey = meta.smallest.userKey();
			Slice maxUserKey = meta.largest.userKey();
			if (base != null)
				level = base.pickLevelForMemTableOutput(minUserKey, maxUserKey);
			
			edit.addFile(level, meta.number, meta.fileSize, meta.smallest, meta.largest, meta.numEntries);
		}

		Logger0.log0(options.infoLog, "Level-0 table #{}: {} bytes Level-{} {}", meta.number, meta.fileSize, level, s);

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
	 *            compact even if there is room
	 * @return
	 */
	Status makeRoomForWrite(boolean force) {
		mutex.assertHeld();
		assert (!writers.isEmpty());
		Status s = Status.ok0();
		try {
			boolean allowDelay = !force; // do not allow delay if force is true.
			while (true) {
				if (!bgError.ok()) {
					// Yield previous error
					s = bgError.clone();
					break;
				} else if (allowDelay && versions.numLevelFiles(0) >= DBFormat.kL0_SlowdownWritesTrigger) {
					// We are getting close to hitting a hard limit on the number of
					// L0 files. Rather than delaying a single write by several
					// seconds when we hit the hard limit, start delaying each
					// individual write by 1ms to reduce latency variance. Also,
					// this delay hands over some CPU to the compaction thread in
					// case it is sharing the same core as the writer.
					mutex.unlock();
					env.sleepForMilliseconds(1);
					allowDelay = false; // Do not delay a single write more than once
					mutex.lock();
				} else if (!force && (memtable.approximateMemoryUsage() <= options.writeBufferSize)) {
					// There is room in current memtable
					break;
				} else if (immtable != null) {
					// We have filled up the current memtable, but the previous
					// one is still being compacted, so we wait.
					Logger0.log0(options.infoLog, "Current memtable full; waiting...\n");
					bgCv.await();
				} else if (versions.numLevelFiles(0) >= DBFormat.kL0_StopWritesTrigger) {
					// There are too many level-0 files.
					Logger0.log0(options.infoLog, "Too many L0 files; waiting...\n");
					bgCv.await();
				} else {
					// Attempt to switch to a new memtable and trigger compaction of old
					assert (versions.prevLogNumber() == 0);
					long newLogNumber = versions.newFileNumber();
					Object0<WritableFile> lfile = new Object0<>();
					s = env.newWritableFile(FileName.getLogFileName(dbname, newLogNumber), lfile);
					if (!s.ok()) {
						// Avoid chewing through file number space in a tight loop.
						versions.reuseFileNumber(newLogNumber);
						break;
					}
					logWriter.delete();
					logWriter = null;
					logFile.delete();
					logFile = null;

					logFile = lfile.getValue();
					logFileNumber = newLogNumber;
					logWriter = new LogWriter(lfile.getValue());
					immtable = memtable;
					hasImm.set(immtable);
					memtable = new MemTable(internalComparator);
					memtable.ref();
					force = false; // Do not force another compaction if have room
					maybeScheduleCompaction();
				}
			}
			return s;
		} catch (Exception e) {
			e.printStackTrace();
			return Status.otherError("" + e);
		}
	}

	WriteBatch buildBatchGroup(Object0<Writer> lastWriterOut) {
		assert (!writers.isEmpty());
		Writer first = writers.peekFirst();
		WriteBatch result = first.batch;
		assert (result != null);

		long size = WriteBatchInternal.byteSize(first.batch);

		// Allow the group to grow up to a maximum size, but if the
		// original write is small, limit the growth so we do not slow
		// down the small write too much.
		long maxSize = 1 << 20;
		if (size <= (128 << 10)) {
			maxSize = size + (128 << 10);
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

				// Append to result
				if (result == first.batch) {
					// Switch to temporary batch instead of disturbing caller's batch
					result = tmpBatch;
					assert (WriteBatchInternal.count(result) == 0);
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
		} else if (shuttingDown.get() != null) {
			// DB is being deleted; no more background compactions
		} else if (!bgError.ok()) {
			// Already got an error; no more changes
		} else if (immtable == null && manualCompaction == null && !versions.needsCompaction()) {
			// No work to be done
		} else {
			bgCompactionScheduled = true;
			env.schedule(new BgWorkRunnable());
		}
	}

	public class BgWorkRunnable implements Runnable {
		public void run() {
			backgroundCall();
		}
	}

	void backgroundCall() {
		mutex.lock();
		try {
			assert (bgCompactionScheduled);
			if (shuttingDown.get() != null) {
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
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			mutex.unlock();
		}
	}

	/**
	 * EXCLUSIVE_LOCKS_REQUIRED(mutex)
	 */
	void backgroundCompaction() {
		mutex.assertHeld();

		if (immtable != null) {
			compactMemTable();
			return;
		}

		Compaction c;
		boolean isManual = (manualCompaction != null);
		InternalKey manualEnd = new InternalKey();
		if (isManual) {
			ManualCompaction m = manualCompaction;
			c = versions.compactRange(m.level, m.begin, m.end);
			m.done = (c == null);
			if (c != null)
				manualEnd = c.input(0, c.numInputFiles(0) - 1).largest;

			Logger0.log0(options.infoLog, "Manual compaction at level-{} from {} .. {}; will stop at {}\n", 
					m.level, (m.begin != null ? m.begin.debugString() : "(begin)"),
					(m.end != null ? m.end.debugString() : "(end)"), (m.done ? "(end)" : manualEnd.debugString()));
		} else {
			c = versions.pickCompaction();
		}

		Status status = Status.ok0();
		if (c == null) {
			// Nothing to do
		} else if (!isManual && c.isTrivialMove()) {
			// Move file to next level
			assert (c.numInputFiles(0) == 1);
			FileMetaData f = c.input(0, 0);
			c.edit().deleteFile(c.level(), f.number);
			c.edit().addFile(c.level() + 1, f.number, f.fileSize, f.smallest, f.largest, f.numEntries);
			status = versions.logAndApply(c.edit(), mutex);
			if (!status.ok())
				recordBackgroundError(status);

			Logger0.log0(options.infoLog, "Moved #{} to level-{} {} bytes {}: {}\n", f.number, c.level() + 1, f.fileSize, status, versions.levelSummary());
		} else {

			CompactionState compact = new CompactionState(c);
			status = doCompactionWork(compact);


			if (!status.ok()) {
				recordBackgroundError(status);
			}
			cleanupCompaction(compact);
			c.releaseInputs();
			deleteObsoleteFiles();
		}


		if (c != null)
			c.delete();
		c = null;

		if (status.ok()) {
			// Done
		} else if (shuttingDown.get() != null) { // shutting_down_.Acquire_Load()
			// Ignore compaction errors found during shutting down
		} else {
			Logger0.log0(options.infoLog, "Compaction error: {}", status);
		}

		if (isManual) {
			ManualCompaction m = manualCompaction;
			if (!status.ok()) {
				m.done = true;
			}
			if (!m.done) {
				// We only compacted part of the requested range. Update m
				// to the range that is left to be compacted.
				m.tmpStorage = manualEnd;
				m.begin = m.tmpStorage;
			}
			manualCompaction = null;
		}
	}

	/**
	 * EXCLUSIVE_LOCKS_REQUIRED(mutex)
	 * 
	 * @param compact
	 */
	void cleanupCompaction(CompactionState compact) {
		mutex.assertHeld();
		if (compact.builder != null) {
			// May happen if we get a shutdown call in the middle of compaction
			compact.builder.abandon();
			compact.builder = null;
		} else {
			assert (compact.outFile == null);
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
		assert (compact != null);
		assert (compact.builder == null);

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
		Object0<WritableFile> resultOutFile = new Object0<WritableFile>();
		Status s = env.newWritableFile(fname, resultOutFile);
		compact.outFile = resultOutFile.getValue();
		if (s.ok())
			compact.builder = new TableBuilder(options, compact.outFile);

		return s;
	}

	Status finishCompactionOutputFile(CompactionState compact, Iterator0 input) {
		assert (compact != null);
		assert (compact.outFile != null);
		assert (compact.builder != null);

		final long outputNumber = compact.currentOutput().number;
		assert (outputNumber != 0);

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
			Iterator0 iter = tableCache.newIterator(new ReadOptions(), outputNumber, currentBytes);

			s = iter.status();
			iter.delete();
			iter = null;
			if (s.ok()) {
				Logger0.log0(options.infoLog, "Generated table #{}@{}: {} keys, {} bytes", outputNumber, compact.compaction.level(), currentEntries, currentBytes);
			}
		}

		return s;
	}

	/**
	 * EXCLUSIVE_LOCKS_REQUIRED(mutex)
	 * 
	 * @param compact
	 * @return
	 */
	Status installCompactionResults(CompactionState compact) {
		mutex.assertHeld();
		Logger0.log0(options.infoLog, "Compacted {}@{} + {}@{} files => {} bytes", compact.compaction.numInputFiles(0), compact.compaction.level(), compact.compaction.numInputFiles(1),
				compact.compaction.level() + 1, compact.totalBytes);

		// Add compaction outputs
		compact.compaction.addInputDeletions(compact.compaction.edit());
		int level = compact.compaction.level();
		for (int i = 0; i < compact.outputs.size(); i++) {
			CompactionState.Output out = compact.outputs.get(i);
			compact.compaction.edit().addFile(level + 1, out.number, out.fileSize, out.smallest, out.largest, out.numEntries);
		}

		return versions.logAndApply(compact.compaction.edit(), mutex);
	}

	/**
	 * EXCLUSIVE_LOCKS_REQUIRED(mutex)
	 */
	Status doCompactionWork(CompactionState compact) {
		long startMillis = env.nowMillis();
		long immMillis = 0; // millis spent doing imm compactions

		Logger0.log0(options.infoLog, "Compacting {}@{} + {}@{} files", compact.compaction.numInputFiles(0), compact.compaction.level(), compact.compaction.numInputFiles(1),
				compact.compaction.level() + 1);

		assert (versions.numLevelFiles(compact.compaction.level()) > 0);
		assert (compact.builder == null);
		assert (compact.outFile == null);
		if (snapshots.isEmpty()) {
			compact.smallestSnapshot = versions.lastSequence;
		} else {
			compact.smallestSnapshot = snapshots.oldest().number;
		}

		// Release mutex while we're actually doing the compaction work
		mutex.unlock();

		Status status = Status.ok0();
		ParsedInternalKey ikey = new ParsedInternalKey();
		ByteBuf currentUserKey = ByteBufFactory.newUnpooled();
		boolean hasCurrentUserKey = false;
		long lastSequenceForKey = DBFormat.kMaxSequenceNumber;

		Iterator0 input = versions.makeInputIterator(compact.compaction);
		input.seekToFirst();
		for (; input.valid() && shuttingDown.get() == null;) { // shutting_down_.Acquire_Load()

			// Prioritize immutable compaction work
			if (hasImm.get() != null) { // has_imm_.NoBarrier_Load() != NULL
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
			if (compact.compaction.shouldStopBefore(key) && compact.builder != null) {
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
				if (!hasCurrentUserKey || userComparator().compare(ikey.userKey, currentUserKey) != 0) {
					// First occurrence of this user key
					currentUserKey.assign(ikey.userKey.data(), ikey.userKey.offset(), ikey.userKey.size());
					hasCurrentUserKey = true;
					lastSequenceForKey = DBFormat.kMaxSequenceNumber;
				}

				if (lastSequenceForKey <= compact.smallestSnapshot) {
					// Hidden by an newer entry for same user key
					drop = true;
				} else if (ikey.type == ValueType.Deletion && ikey.sequence <= compact.smallestSnapshot && compact.compaction.isBaseLevelForKey(ikey.userKey)) {
					// For this user key:
					// (1) there is no data in higher levels
					// (2) data in lower levels will have larger sequence numbers
					// (3) data in layers that are being compacted here and have
					// smaller sequence numbers will be dropped in the next
					// few iterations of this loop (by rule (A) above).
					// Therefore this deletion marker is obsolete and can be dropped.
					drop = true;
				}

				lastSequenceForKey = ikey.sequence;
			}

			if (!drop) {
				// Open output file if necessary
				if (compact.builder == null) {
					status = this.openCompactionOutputFile(compact);
					if (!status.ok())
						break;
				}

				if (compact.builder.numEntries() == 0) {
					compact.currentOutput().smallest.decodeFrom(key);
					compact.currentOutput().numEntries = 0;
				}
				compact.currentOutput().largest.decodeFrom(key);

				compact.builder.add(key, input.value());

				compact.currentOutput().numEntries++;

				// Close output file if it is big enough
				if (compact.builder.fileSize() >= compact.compaction.maxOutputFileSize()) {
					status = finishCompactionOutputFile(compact, input);
					if (!status.ok())
						break;
				}
			}

			input.next();
		}

		if (status.ok() && shuttingDown.get() != null)
			status = new Status(Status.Code.IOError, "Deleting DB during compaction");

		if (status.ok() && compact.builder != null)
			status = finishCompactionOutputFile(compact, input);

		if (status.ok())
			status = input.status();

		input.delete();
		input = null;

		CompactionStats stat = new CompactionStats();
		stat.millis = env.nowMillis() - startMillis - immMillis;
		for (int which = 0; which < 2; which++) {
			for (int i = 0; i < compact.compaction.numInputFiles(which); i++) {
				stat.bytesRead += compact.compaction.input(which, i).fileSize;
			}
		}
		for (int i = 0; i < compact.outputs.size(); i++)
			stat.bytesWritten += compact.outputs.get(i).fileSize;

		mutex.lock();
		stats[compact.compaction.level() + 1].add(stat);

		if (status.ok())
			status = installCompactionResults(compact);

		if (!status.ok())
			recordBackgroundError(status);

		Logger0.log0(options.infoLog, "compacted to: {}", versions.levelSummary());
		return status;
	}

	public Comparator0 userComparator() {
		return internalComparator.userComparator();
	}

	/**
	 * Record a sample of bytes read at the specified internal key. Samples are
	 * taken approximately once every config::kReadBytesPeriod bytes.
	 * 
	 * @param key
	 */
	public void recordReadSample(Slice key) {
		mutex.lock();
		try {
			if (versions.current().recordReadSample(key))
				maybeScheduleCompaction();
		} finally {
			mutex.unlock();
		}
	}

	Iterator0 newInternalIterator(ReadOptions options, Long0 latestSnapshot, Integer0 seed0) {
		IterState cleanup = new IterState();
		mutex.lock();
		latestSnapshot.setValue(versions.lastSequence());

		// Collect together all needed child iterators
		ArrayList<Iterator0> list = new ArrayList<>();
		list.add(memtable.newIterator());
		memtable.ref();
		if (immtable != null) {
			list.add(immtable.newIterator());
			immtable.ref();
		}
		versions.current().addIterators(options, list);
		Iterator0 internalIter = MergingIterator.newMergingIterator(internalComparator, list);
		versions.current().ref();

		cleanup.mutex = mutex;
		cleanup.mem = memtable;
		cleanup.imm = immtable;
		cleanup.version = versions.current();
		internalIter.registerCleanup(new CleanupIteratorState(cleanup));

		seed0.setValue(++seed);
		mutex.unlock();
		return internalIter;
	}

	public Iterator0 newIterator(ReadOptions options) {
		Long0 latestSnapshot = new Long0();
		Integer0 seed0 = new Integer0();
		Iterator0 iter = newInternalIterator(options, latestSnapshot, seed0);
		return DBIter.newDBIterator(this, userComparator(), iter, (options.snapshot != null ? ((Snapshot) (options.snapshot)).number : latestSnapshot.getValue()), seed0.getValue());
	}

	public boolean getProperty(String property, Object0<String> value) {
		value.setValue("");
		mutex.lock();
		try {
			String in = property;
			String prefix = "leveldb.";
			if (!in.startsWith(prefix))
				return false;

			in = in.substring(prefix.length());

			if (in.startsWith("num-files-at-level")) {
				in = in.substring("num-files-at-level".length());
				int level = Integer.parseInt(in);
				if (level >= DBFormat.kNumLevels) {
					return false;
				} else {
					value.setValue("" + versions.numLevelFiles((int) level));
					return true;
				}
			} else if (in.equals("stats")) {
				String s = "";
				s += "                               Compactions\n";
				s += "Level  Files Size(MB) Time(sec) Read(MB) Write(MB)\n";
				s += "--------------------------------------------------\n";

				for (int level = 0; level < DBFormat.kNumLevels; level++) {
					int files = versions.numLevelFiles(level);
					if (stats[level].millis > 0 || files > 0) {
						s += String.format("%3d %8d %8.0f %9.0f %8.0f %9.0f\n", level, files, versions.numLevelBytes(level) / 1048576.0, stats[level].millis / 1e3, stats[level].bytesRead / 1048576.0,
								stats[level].bytesWritten / 1048576.0);
					}
				}
				value.setValue(s);
				return true;
			} else if (in.equals("sstables")) {
				value.setValue(versions.current().debugString());
			} else if (in.equals("approximate-memory-usage")) {
				long totalUsage = options.blockCache.totalCharge();
				if (memtable != null) {
					totalUsage += memtable.approximateMemoryUsage();
				}
				if (immtable != null) {
					totalUsage += immtable.approximateMemoryUsage();
				}
				value.setValue("" + totalUsage);
				return true;
			}

			return false;
		} finally {
			mutex.unlock();
		}
	}

	@Override
	public Snapshot getSnapshot() {
		mutex.lock();
		try {
			return snapshots.new0(versions.lastSequence());
		} finally {
			mutex.unlock();
		}
	}

	public void releaseSnapshot(Snapshot snapshot) {
		mutex.lock();
		try {
			snapshots.delete(snapshot);
		} finally {
			mutex.unlock();
		}
	}

	public void compactRange(Slice begin, Slice end) throws Exception {
		int maxLevelWithFiles = 1;
		mutex.lock();
		try {
			Version base = versions.current();
			for (int level = 1; level < DBFormat.kNumLevels; level++) {
				if (base.overlapInLevel(level, begin, end)) {
					maxLevelWithFiles = level;
				}
			}

			TEST_CompactMemTable(); // TODO: Skip if memtable does not overlap
			for (int level = 0; level < maxLevelWithFiles; level++) {
				TEST_CompactRange(level, begin, end);
			}
		} finally {
			mutex.unlock();
		}
	}

	public void TEST_CompactRange(int level, Slice begin, Slice end) throws Exception {

		assert (level >= 0);
		assert (level + 1 < DBFormat.kNumLevels);

		InternalKey begin_storage = null;
		InternalKey end_storage = null;

		ManualCompaction manual = new ManualCompaction();
		manual.level = level;
		manual.done = false;
		if (begin == null) {
			manual.begin = null;
		} else {
			begin_storage = new InternalKey(begin.clone(), DBFormat.kMaxSequenceNumber, DBFormat.kValueTypeForSeek);
			manual.begin = begin_storage;
		}

		if (end == null) {
			manual.end = null;
		} else {
			end_storage = new InternalKey(end.clone(), 0, ValueType.Deletion); // end_storage = InternalKey(*end, 0, static_cast<ValueType>(0));
			manual.end = end_storage;
		}

		mutex.lock();
		try {
			while (!manual.done && shuttingDown.get() == null && bgError.ok()) {
				if (manualCompaction == null) { // Idle
					manualCompaction = manual;
					maybeScheduleCompaction();
				} else { // Running either my compaction or another compaction.
					bgCv.await();
				}
			}
			if (manualCompaction == manual) {
				// Cancel my manual compaction since we aborted early for some reason.
				manualCompaction = null;
			}
		} finally {
			mutex.unlock();
		}
	}

	public Status TEST_CompactMemTable() throws Exception {
		// NULL batch means just wait for earlier writes to be done
		Status s = write(new WriteOptions(), null);
		if (s.ok()) {
			// Wait until the compaction completes
			mutex.lock();
			try {
				while (immtable != null && bgError.ok()) {
					bgCv.await();
				}
				if (immtable != null) {
					s = bgError;
				}
			} catch (Exception e) {
				return Status.otherError("" + e);
			} finally {
				mutex.unlock();
			}
		}
		return s;
	}

	@Override
	public void getApproximateSizes(List<Range> rangeList, List<Long> sizes) {
		// TODO(opt): better implementation
		Version v;
		try {
			mutex.lock();
			versions.current().ref();
			v = versions.current();
		} finally {
			mutex.unlock();
		}

		for (int i = 0; i < rangeList.size(); i++) {
			// Convert user_key into a corresponding internal key.
			Range r = rangeList.get(i);
			InternalKey k1 = new InternalKey(r.start, DBFormat.kMaxSequenceNumber, DBFormat.kValueTypeForSeek);
			InternalKey k2 = new InternalKey(r.limit, DBFormat.kMaxSequenceNumber, DBFormat.kValueTypeForSeek);
			long start = versions.approximateOffsetOf(v, k1);
			long limit = versions.approximateOffsetOf(v, k2);
			sizes.add((limit >= start ? limit - start : 0));
		}

		try {
			mutex.lock();
			v.unref();
		} finally {
			mutex.unlock();
		}
	}

	@Override
	public String debugDataRange() {
		StringBuilder s = new StringBuilder();

		{
			s.append("Memtable: (");
			Iterator0 memit = memtable.newIterator();

			memit.seekToFirst();
			if (!memit.valid()) {
				s.append("null");
			} else {
				s.append(memit.key().escapeString());
			}
			s.append(", ");
			memit.seekToLast();
			if (!memit.valid()) {
				s.append("null");
			} else {
				s.append(memit.key().escapeString());
			}

			s.append(")\n");

			memit.delete();
			memit = null;
		}

		{
			if (immtable != null) {
				s.append("Immtable: (");
				Iterator0 memit = immtable.newIterator();

				memit.seekToFirst();
				if (!memit.valid()) {
					s.append("null");
				} else {
					s.append(memit.key().escapeString());
				}
				s.append(", ");
				memit.seekToLast();
				if (!memit.valid()) {
					s.append("null");
				} else {
					s.append(memit.key().escapeString());
				}

				s.append(")\n");

				memit.delete();
				memit = null;
			}
		}

		{
			s.append("Files: (\n");
			s.append(versions.debugDataRange());
			s.append(")\n");
		}

		return s.toString();
	}

	public Iterator0 TEST_NewInternalIterator() {
		Long0 latestSnapshot = new Long0();
		Integer0 seed0 = new Integer0();
		return newInternalIterator(new ReadOptions(), latestSnapshot, seed0);
	}

	public long TEST_MaxNextLevelOverlappingBytes() {
		mutex.lock();
		try {
			return versions.maxNextLevelOverlappingBytes();
		} finally {
			mutex.unlock();
		}
	}
}
