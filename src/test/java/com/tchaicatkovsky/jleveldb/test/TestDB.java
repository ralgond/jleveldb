package com.tchaicatkovsky.jleveldb.test;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Test;

import com.tchaicatkovsky.jleveldb.CompressionType;
import com.tchaicatkovsky.jleveldb.DB;
import com.tchaicatkovsky.jleveldb.Env;
import com.tchaicatkovsky.jleveldb.EnvWrapper;
import com.tchaicatkovsky.jleveldb.FileName;
import com.tchaicatkovsky.jleveldb.FileType;
import com.tchaicatkovsky.jleveldb.FilterPolicy;
import com.tchaicatkovsky.jleveldb.Iterator0;
import com.tchaicatkovsky.jleveldb.LevelDB;
import com.tchaicatkovsky.jleveldb.Options;
import com.tchaicatkovsky.jleveldb.RandomAccessFile0;
import com.tchaicatkovsky.jleveldb.Range;
import com.tchaicatkovsky.jleveldb.ReadOptions;
import com.tchaicatkovsky.jleveldb.Snapshot;
import com.tchaicatkovsky.jleveldb.Status;
import com.tchaicatkovsky.jleveldb.WritableFile;
import com.tchaicatkovsky.jleveldb.WriteBatch;
import com.tchaicatkovsky.jleveldb.WriteOptions;
import com.tchaicatkovsky.jleveldb.db.DBImpl;
import com.tchaicatkovsky.jleveldb.db.VersionEdit;
import com.tchaicatkovsky.jleveldb.db.VersionSet;
import com.tchaicatkovsky.jleveldb.db.format.DBFormat;
import com.tchaicatkovsky.jleveldb.db.format.InternalKey;
import com.tchaicatkovsky.jleveldb.db.format.InternalKeyComparator;
import com.tchaicatkovsky.jleveldb.db.format.ParsedInternalKey;
import com.tchaicatkovsky.jleveldb.db.format.ValueType;
import com.tchaicatkovsky.jleveldb.util.BloomFilterPolicy;
import com.tchaicatkovsky.jleveldb.util.Boolean0;
import com.tchaicatkovsky.jleveldb.util.ByteBuf;
import com.tchaicatkovsky.jleveldb.util.ByteBufFactory;
import com.tchaicatkovsky.jleveldb.util.BytewiseComparatorImpl;
import com.tchaicatkovsky.jleveldb.util.Cache;
import com.tchaicatkovsky.jleveldb.util.Comparator0;
import com.tchaicatkovsky.jleveldb.util.DefaultSlice;
import com.tchaicatkovsky.jleveldb.util.ListUtils;
import com.tchaicatkovsky.jleveldb.util.Long0;
import com.tchaicatkovsky.jleveldb.util.Mutex;
import com.tchaicatkovsky.jleveldb.util.Object0;
import com.tchaicatkovsky.jleveldb.util.Random0;
import com.tchaicatkovsky.jleveldb.util.Slice;
import com.tchaicatkovsky.jleveldb.util.Strings;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestDB {

	static ByteBuf randomString(Random0 rnd, int len) {
		ByteBuf r = ByteBufFactory.defaultByteBuf();
		TestUtil.randomString(rnd, len, r);
		return r;
	}

	void delayMilliseconds(int millis) {
		LevelDB.defaultEnv().sleepForMilliseconds(millis);
	}

	// Special Env used to delay background operations
	static class SpecialEnv extends EnvWrapper {
		// sstable/log Sync() calls are blocked while this pointer is non-NULL.
		AtomicReference<Object> delayDataSync = new AtomicReference<Object>();

		// sstable/log Sync() calls return an error.
		AtomicReference<Object> dataSyncError = new AtomicReference<Object>();

		// Simulate no-space errors while this pointer is non-NULL.
		AtomicReference<Object> noSpace = new AtomicReference<Object>();

		// Simulate non-writable file system while this pointer is non-NULL
		AtomicReference<Object> nonWritable = new AtomicReference<Object>();

		// Force sync of manifest files to fail while this pointer is non-NULL
		AtomicReference<Object> manifestSyncError = new AtomicReference<Object>();

		// Force write to manifest files to fail while this pointer is non-NULL
		AtomicReference<Object> manifestWriteError = new AtomicReference<Object>();

		boolean countRandomReads;
		AtomicLong randomReadCounter = new AtomicLong();

		public SpecialEnv(Env base) {
			super(base);
			delayDataSync.set(null);
			dataSyncError.set(null);
			noSpace.set(null);
			nonWritable.set(null);
			countRandomReads = false;
			manifestSyncError.set(null);
			manifestWriteError.set(null);
		}

		static class DataFile implements WritableFile {
			SpecialEnv env;
			WritableFile base;

			public DataFile(SpecialEnv env, WritableFile base) {
				this.env = env;
				this.base = base;
			}

			@Override
			public void delete() {
				if (base != null) {
					base.delete();
					base = null;
				}
			}

			@Override
			public Status append(Slice data) {
				if (env.noSpace.get() != null) {
					// Drop writes on the floor
					return Status.ok0();
				} else {
					return base.append(data);
				}
			}

			@Override
			public Status close() {
				return base.close();
			}

			@Override
			public Status flush() {
				return base.flush();
			}

			@Override
			public Status sync() {
				if (env.dataSyncError.get() != null) {
					return Status.ioError("simulated data sync error");
				}
				while (env.delayDataSync.get() != null) {
					env.sleepForMilliseconds(100);
				}
				return base.sync();
			}
		};

		public static class ManifestFile implements WritableFile {
			SpecialEnv env;
			WritableFile base;

			public ManifestFile(SpecialEnv env, WritableFile b) {
				this.env = env;
				base = b;
			}

			@Override
			public void delete() {
				base.delete();
				base = null;
			}

			@Override
			public Status append(Slice data) {
				if (env.manifestWriteError.get() != null) {
					return Status.ioError("simulated writer error");
				} else {
					return base.append(data);
				}
			}

			@Override
			public Status close() {
				return base.close();
			}

			@Override
			public Status flush() {
				return base.flush();
			}

			@Override
			public Status sync() {
				if (env.manifestSyncError.get() != null) {
					return Status.ioError("simulated sync error");
				} else {
					return base.sync();
				}
			}
		};

		@Override
		public Status newWritableFile(String f, Object0<WritableFile> r) {
			if (nonWritable.get() != null) {
				return Status.ioError("simulated write error");
			}

			Status s = target().newWritableFile(f, r);
			if (s.ok()) {
				if (f.indexOf(".ldb") >= 0 || f.indexOf(".log") >= 0) {
					r.setValue(new DataFile(this, r.getValue()));
				} else if (f.indexOf("MANIFEST") >= 0) {
					r.setValue(new ManifestFile(this, r.getValue()));
				}
			}
			return s;
		}

		static class CountingFile implements RandomAccessFile0 {
			RandomAccessFile0 target;
			AtomicLong counter;

			public CountingFile(RandomAccessFile0 target, AtomicLong counter) {
				this.target = target;
				this.counter = counter;
			}

			public String name() {
				if (target != null)
					return target.name();
				else
					return null;
			}

			@Override
			public void delete() {
				target.delete();
				target = null;
			}

			@Override
			public void close() {
				target.close();
			}

			@Override
			public Status read(long offset, int n, Slice result, byte[] scratch) {
				counter.incrementAndGet();
				return target.read(offset, n, result, scratch);
			}
		};

		@Override
		public Status newRandomAccessFile(String f, Object0<RandomAccessFile0> r) {
			Status s = target().newRandomAccessFile(f, r);
			if (s.ok() && countRandomReads) {
				r.setValue(new CountingFile(r.getValue(), randomReadCounter));
			}
			return s;
		}
	};

	static class DBTestRunner {
		FilterPolicy filterPolicy;

		// Sequence of option configurations to try
		enum OptionConfig {
			kDefault, kReuse, kFilter, kUncompressed, kEnd
		};

		public int optionConfig;

		String dbname;
		SpecialEnv env;
		DB db;

		Options lastOptions;

		public DBTestRunner() throws Exception {
			optionConfig = OptionConfig.kDefault.ordinal();
			env = new SpecialEnv(LevelDB.defaultEnv());
			filterPolicy = BloomFilterPolicy.newBloomFilterPolicy(10);
			dbname = TestUtil.tmpDir() + "/dbtest";
			LevelDB.destroyDB(dbname, new Options());
			db = null;
			optionConfig = 0;
			reopen();
		}

		public void delete() throws Exception {
			if (db != null)
				db.close();
			LevelDB.destroyDB(dbname, new Options());
			filterPolicy = null;
		}

		// Switch to a fresh database with the next option configuration to
		// test. Return false if there are no more configurations to test.
		public boolean changeOptions() throws Exception {
			optionConfig++;
			if (optionConfig >= OptionConfig.kEnd.ordinal()) {
				return false;
			} else {
				destroyAndReopen();
				return true;
			}
		}

		// Return the current option configuration.
		public Options currentOptions() {
			Options options = new Options();
			options.reuseLogs = false;
			if (optionConfig == OptionConfig.kReuse.ordinal()) {
				options.reuseLogs = true;
			} else if (optionConfig == OptionConfig.kFilter.ordinal()) {
				options.filterPolicy = filterPolicy;
			} else if (optionConfig == OptionConfig.kUncompressed.ordinal()) {
				options.compression = CompressionType.kNoCompression;
			}
			return options;
		}

		public DBImpl dbfull() {
			return (DBImpl) (db);
		}

		public void reopen(Options options) throws Exception {
			assertTrue(tryReopen(options).ok());
		}

		public void reopen() throws Exception {
			reopen(null);
		}

		public void close() {
			db.close();
			db = null;
		}

		public void destroyAndReopen(Options options) throws Exception {
			db.close();
			db = null;
			LevelDB.destroyDB(dbname, new Options());
			assertTrue(tryReopen(options).ok());
		}

		public void destroyAndReopen() throws Exception {
			destroyAndReopen(null);
		}

		OptionConfig getOptionConfig(int i) {
			if (i == OptionConfig.kDefault.ordinal())
				return OptionConfig.kDefault;
			else if (i == OptionConfig.kReuse.ordinal())
				return OptionConfig.kReuse;
			else if (i == OptionConfig.kFilter.ordinal())
				return OptionConfig.kFilter;
			else if (i == OptionConfig.kUncompressed.ordinal())
				return OptionConfig.kUncompressed;
			else
				return null;
		}

		public Status tryReopen(Options options) throws Exception {
			System.out.println("\n\n");
			System.out.println("[DEBUG] ===================================================");
			System.out.println("[DEBUG] optionsConfig=" + optionConfig + ", " + getOptionConfig(optionConfig).name());
			System.out.println("[DEBUG] ===================================================");

			if (db != null) {
				db.close();
				db = null;
			}
			Options opts;
			if (options != null) {
				opts = options.cloneOptions();
			} else {
				opts = currentOptions().cloneOptions();
				opts.createIfMissing = true;
			}
			lastOptions = opts.cloneOptions();

			Object0<DB> db0 = new Object0<>();
			Status s = LevelDB.newDB(opts, dbname, db0);
			db = db0.getValue();
			System.out.println("[DEBUG] DBTestRunner.tryReopen, s=" + s);
			if (s.ok())
				System.out.printf("[DEBUG] tryReopen after open, dataRange=%s", db.debugDataRange());
			return s;
		}

		public Status put(String k, Slice v) throws Exception {
			return db.put(new WriteOptions(), new DefaultSlice(k), v);
		}

		public Status put(Slice k, Slice v) throws Exception {
			return db.put(new WriteOptions(), k, v);
		}

		public Status put(String k, String v) throws Exception {
			return db.put(new WriteOptions(), new DefaultSlice(k), new DefaultSlice(v));
		}

		public Status delete(Slice k) throws Exception {
			return db.delete(new WriteOptions(), k);
		}

		public Status delete(String k) throws Exception {
			return db.delete(new WriteOptions(), new DefaultSlice(k));
		}

		public String get(Slice k, ReadOptions options, Snapshot snapshot) throws Exception {
			options.snapshot = snapshot;
			ByteBuf result = ByteBufFactory.defaultByteBuf();
			Status s = db.get(options, k, result);
			if (s.isNotFound()) {
				return "NOT_FOUND";
			} else if (!s.ok()) {
				return "" + s;
			}
			return Strings.escapeString(result);
		}

		public String get(Slice k, Snapshot snapshot) throws Exception {
			return get(k, new ReadOptions(), snapshot);
		}

		public String get(String k, Snapshot snapshot) throws Exception {
			return get(new DefaultSlice(k), snapshot);
		}

		public String get(Slice k) throws Exception {
			return get(k, null);
		}

		public String get(String k) throws Exception {
			return get(new DefaultSlice(k), null);
		}

		public String get(String k, ReadOptions options) throws Exception {
			return get(new DefaultSlice(k), options, null);
		}

		String iterStatus(Iterator0 iter) {
			String result = "";
			if (iter.valid()) {
				result = Strings.escapeString(iter.key()) + "->" + Strings.escapeString(iter.value());
			} else {
				result = "(invalid)";
			}
			return result;
		}

		// Return a string that contains all key,value pairs in order,
		// formatted like "(k1->v1)(k2->v2)".
		public String contents() {
			ArrayList<String> forward = new ArrayList<String>();
			StringBuilder result = new StringBuilder();
			Iterator0 iter = db.newIterator(new ReadOptions());
			for (iter.seekToFirst(); iter.valid(); iter.next()) {
				String s = iterStatus(iter);
				result.append('(');
				result.append(s);
				result.append(')');
				forward.add(s);
			}

			// Check reverse iteration results are the reverse of forward
			// results
			int matched = 0;
			for (iter.seekToLast(); iter.valid(); iter.prev()) {
				assertTrue(matched < forward.size());
				assertEquals(iterStatus(iter), forward.get(forward.size() - matched - 1));
				matched++;
			}
			assertEquals(matched, forward.size());

			iter.delete();
			return result.toString();
		}

		public String allEntriesFor(Slice user_key) {
			Iterator0 iter = dbfull().TEST_NewInternalIterator();
			InternalKey target = new InternalKey(user_key, DBFormat.kMaxSequenceNumber, ValueType.Value);
			iter.seek(target.encode());
			StringBuilder result = new StringBuilder();
			if (!iter.status().ok()) {
				result.append(iter.status().toString());
			} else {
				result.append("[ ");
				boolean first = true;
				while (iter.valid()) {
					ParsedInternalKey ikey = new ParsedInternalKey();
					if (!ikey.parse(iter.key())) {
						result.append("CORRUPTED");
					} else {
						if (lastOptions.comparator.compare(ikey.userKey, user_key) != 0) {
							break;
						}
						if (!first) {
							result.append(", ");
						}
						first = false;
						switch (ikey.type) {
						case Value:
							result.append(Strings.escapeString(iter.value()));
							break;
						case Deletion:
							result.append("DEL");
							break;
						}
					}
					iter.next();
				}
				if (!first) {
					result.append(" ");
				}
				result.append("]");
			}
			iter.delete();
			return result.toString();
		}

		public int numTableFilesAtLevel(int level) {
			Object0<String> property = new Object0<String>();
			assertTrue(db.getProperty("leveldb.num-files-at-level" + level, property));
			return Integer.parseInt(property.getValue());
		}

		public int totalTableFiles() {
			int result = 0;
			for (int level = 0; level < DBFormat.kNumLevels; level++) {
				result += numTableFilesAtLevel(level);
			}
			return result;
		}

		// Return spread of files per level
		public String filesPerLevel() {
			int last_non_zero_offset = 0;
			StringBuilder result = new StringBuilder();
			for (int level = 0; level < DBFormat.kNumLevels; level++) {
				int f = numTableFilesAtLevel(level);
				String s = String.format("%s%d", (level != 0 ? "," : ""), f);
				result.append(s);
				if (f > 0) {
					last_non_zero_offset = result.toString().length();
				}
			}
			return result.toString().substring(0, last_non_zero_offset);
		}

		public int countFiles() {
			ArrayList<String> files = new ArrayList<>();
			env.getChildren(dbname, files);
			for (String fileName : files)
				System.out.println("DB FILENAME: " + fileName);
			return files.size();
		}

		public long size(Slice start, Slice limit) {
			Range r = new Range(start, limit);
			ArrayList<Long> sizes = new ArrayList<>();
			ArrayList<Range> l = new ArrayList<>();
			l.add(r);
			db.getApproximateSizes(l, sizes);
			return sizes.get(0);
		}

		public void compact(Slice start, Slice limit) throws Exception {
			db.compactRange(start, limit);
		}

		public void compact(String start, String limit) throws Exception {
			db.compactRange(new DefaultSlice(start), new DefaultSlice(limit));
		}

		// Do n memtable compactions, each of which produces an sstable
		// covering the range [small,large].
		public void makeTables(int n, String small, String large) throws Exception {
			for (int i = 0; i < n; i++) {
				put(new DefaultSlice(small), new DefaultSlice("begin"));
				put(new DefaultSlice(large), new DefaultSlice("end"));
				dbfull().TEST_CompactMemTable();
			}
		}

		// Prevent pushing of new sstables into deeper levels by adding
		// tables that cover a specified range to all levels.
		public void fillLevels(String smallest, String largest) throws Exception {
			makeTables(DBFormat.kNumLevels, smallest, largest);
		}

		public void dumpFileCounts(String label) {
			System.err.printf("---\n%s:\n", label);
			System.err.printf("maxoverlap: %d\n", dbfull().TEST_MaxNextLevelOverlappingBytes());
			for (int level = 0; level < DBFormat.kNumLevels; level++) {
				int num = numTableFilesAtLevel(level);
				if (num > 0) {
					System.err.printf("  level %3d : %d files\n", level, num);
				}
			}
		}

		public String dumpSSTableList() {
			Object0<String> property = new Object0<>();
			db.getProperty("leveldb.sstables", property);
			return property.getValue();
		}

		public boolean deleteAnSSTFile() {
			ArrayList<String> filenames = new ArrayList<String>();
			assertTrue(env.getChildren(dbname, filenames).ok());
			Long0 number = new Long0();
			Object0<FileType> type = new Object0<>();
			for (int i = 0; i < filenames.size(); i++) {
				if (FileName.parseFileName(filenames.get(i), number, type) && type.getValue() == FileType.TableFile) {
					assertTrue(env.deleteFile(FileName.getTableFileName(dbname, number.getValue())).ok());
					return true;
				}
			}
			return false;
		}

		// Returns number of files renamed.
		public int renameLDBToSST() {
			ArrayList<String> filenames = new ArrayList<>();
			assertTrue(env.getChildren(dbname, filenames).ok());
			Long0 number = new Long0();
			Object0<FileType> type = new Object0<>();
			int files_renamed = 0;
			for (int i = 0; i < filenames.size(); i++) {
				if (FileName.parseFileName(filenames.get(i), number, type) && type.getValue() == FileType.TableFile) {
					String from = FileName.getTableFileName(dbname, number.getValue());
					String to = FileName.getSSTTableFileName(dbname, number.getValue());
					assertTrue(env.renameFile(from, to).ok());
					files_renamed++;
				}
			}
			return files_renamed;
		}
	}

	@Test
	public void testEmpty() throws Exception {
		DBTestRunner r = new DBTestRunner();
		try {
			do {
				assertTrue(r.db != null);
				assertEquals("NOT_FOUND", r.get(new DefaultSlice("foo")));
			} while (r.changeOptions());
		} finally {
			r.delete();
		}
	}

	@Test
	public void testReadWrite() throws Exception {
		DBTestRunner r = new DBTestRunner();
		try {
			do {
				System.out.println("[DEBUG] r.option_config=" + r.optionConfig);
				assertTrue(r.put("foo", "v1").ok());
				assertEquals("v1", r.get("foo"));
				assertTrue(r.put("bar", "v2").ok());
				assertTrue(r.put("foo", "v3").ok());
				assertEquals("v3", r.get("foo"));
				assertEquals("v2", r.get("bar"));
			} while (r.changeOptions());
		} finally {
			r.delete();
		}
	}

	@Test
	public void testPutDeleteGet() throws Exception {
		DBTestRunner r = new DBTestRunner();
		try {
			do {
				assertTrue(r.db.put(new WriteOptions(), new DefaultSlice("foo"), new DefaultSlice("v1")).ok());
				assertEquals("v1", r.get("foo"));
				assertTrue(r.db.put(new WriteOptions(), new DefaultSlice("foo"), new DefaultSlice("v2")).ok());
				assertEquals("v2", r.get("foo"));
				assertTrue(r.db.delete(new WriteOptions(), new DefaultSlice("foo")).ok());
				assertEquals("NOT_FOUND", r.get("foo"));
			} while (r.changeOptions());
		} finally {
			r.delete();
		}
	}

	@Test
	public void testGetFromImmutableLayer() throws Exception {
		DBTestRunner r = new DBTestRunner();
		try {
			do {
				Options options = r.currentOptions().cloneOptions();
				options.env = r.env;
				options.writeBufferSize = 100000; // Small write buffer
				r.reopen(options);

				assertTrue(r.put("foo", "v1").ok());
				assertEquals("v1", r.get("foo"));

				r.env.delayDataSync.set(r.env); // Block sync calls
				r.put("k1", TestUtil.makeString(100000, 'x')); // Fill memtable
				r.put("k2", TestUtil.makeString(100000, 'y')); // Trigger compaction
				assertEquals("v1", r.get("foo"));
				r.env.delayDataSync.set(null); // Release sync calls
			} while (r.changeOptions());
		} finally {
			r.delete();
		}
	}

	@Test
	public void testGetFromVersions() throws Exception {
		DBTestRunner r = new DBTestRunner();
		try {
			do {
				assertTrue(r.put("foo", "v1").ok());
				r.dbfull().TEST_CompactMemTable();
				assertEquals("v1", r.get("foo"));
			} while (r.changeOptions());
		} finally {
			r.delete();
		}
	}

	@Test
	public void testGetMemUsage() throws Exception {
		DBTestRunner r = new DBTestRunner();
		try {
			do {
				assertTrue(r.put("foo", "v1").ok());
				Object0<String> val = new Object0<String>();
				assertTrue(r.db.getProperty("leveldb.approximate-memory-usage", val));
				int mem_usage = Integer.parseInt(val.getValue());
				assertTrue(mem_usage > 0);
				assertTrue(mem_usage < 5 * 1024 * 1024);
			} while (r.changeOptions());
		} finally {
			r.delete();
		}
	}

	@Test
	public void testGetSnapshot() throws Exception {
		DBTestRunner r = new DBTestRunner();
		try {
			do {
				// Try with both a short key and a long key
				for (int i = 0; i < 2; i++) {
					String key = (i == 0) ? "foo" : TestUtil.makeString(200, 'x');
					assertTrue(r.put(key, "v1").ok());
					Snapshot s1 = r.db.getSnapshot();
					assertTrue(r.put(key, "v2").ok());
					assertEquals("v2", r.get(key));
					assertEquals("v1", r.get(key, s1));
					r.dbfull().TEST_CompactMemTable();
					assertEquals("v2", r.get(key));
					assertEquals("v1", r.get(key, s1));
					r.db.releaseSnapshot(s1);
				}
			} while (r.changeOptions());
		} finally {
			r.delete();
		}
	}

	@Test
	public void testGetLevel0Ordering() throws Exception {
		DBTestRunner r = new DBTestRunner();
		try {
			do {
				// Check that we process level-0 files in correct order. The code
				// below generates two level-0 files where the earlier one comes
				// before the later one in the level-0 file list since the earlier
				// one has a smaller "smallest" key.
				assertTrue(r.put("bar", "b").ok());
				assertTrue(r.put("foo", "v1").ok());
				r.dbfull().TEST_CompactMemTable();
				assertTrue(r.put("foo", "v2").ok());
				r.dbfull().TEST_CompactMemTable();
				assertEquals("v2", r.get("foo"));
			} while (r.changeOptions());
		} finally {
			r.delete();
		}
	}

	@Test
	public void testGetOrderedByLevels() throws Exception {
		DBTestRunner r = new DBTestRunner();
		try {
			do {
				assertTrue(r.put("foo", "v1").ok());
				r.compact(new DefaultSlice("a"), new DefaultSlice("z"));
				assertEquals("v1", r.get("foo"));
				assertTrue(r.put("foo", "v2").ok());
				assertEquals("v2", r.get("foo"));
				r.dbfull().TEST_CompactMemTable();
				assertEquals("v2", r.get("foo"));
			} while (r.changeOptions());
		} finally {
			r.delete();
		}
	}

	@Test
	public void testGetPicksCorrectFile() throws Exception {
		DBTestRunner r = new DBTestRunner();
		try {
			do {
				// Arrange to have multiple files in a non-level-0 level.
				assertTrue(r.put("a", "va").ok());
				r.compact("a", "b");
				assertTrue(r.put("x", "vx").ok());
				r.compact("x", "y");
				assertTrue(r.put("f", "vf").ok());
				r.compact("f", "g");
				assertEquals("va", r.get("a"));
				assertEquals("vf", r.get("f"));
				assertEquals("vx", r.get("x"));
			} while (r.changeOptions());
		} finally {
			r.delete();
		}
	}

	@Test
	public void testGetEncountersEmptyLevel() throws Exception {
		DBTestRunner r = new DBTestRunner();
		try {
			do {
				// Arrange for the following to happen:
				// * sstable A in level 0
				// * nothing in level 1
				// * sstable B in level 2
				// Then do enough get() calls to arrange for an automatic compaction
				// of sstable A. A bug would cause the compaction to be marked as
				// occurring at level 1 (instead of the correct level 0).

				// Step 1: First place sstables in levels 0 and 2
				int compaction_count = 0;
				while (r.numTableFilesAtLevel(0) == 0 || r.numTableFilesAtLevel(2) == 0) {
					assertTrue(compaction_count < 100);
					compaction_count++;
					r.put("a", "begin");
					r.put("z", "end");
					r.dbfull().TEST_CompactMemTable();
				}

				System.out.printf("[DEBUG] =================== level-0.size=%d, level-1=%d, level-2.size=%d\n", r.numTableFilesAtLevel(0), r.numTableFilesAtLevel(1), r.numTableFilesAtLevel(2));

				// Step 2: clear level 1 if necessary.
				r.dbfull().TEST_CompactRange(1, null, null);
				assertEquals(r.numTableFilesAtLevel(0), 1);
				assertEquals(r.numTableFilesAtLevel(1), 0);
				assertEquals(r.numTableFilesAtLevel(2), 1);

				// Step 3: read a bunch of times
				for (int i = 0; i < 1000; i++) {
					assertEquals("NOT_FOUND", r.get("missing"));
				}

				// Step 4: Wait for compaction to finish
				delayMilliseconds(1000);

				assertEquals(r.numTableFilesAtLevel(0), 0);
			} while (r.changeOptions());
		} finally {
			r.delete();
		}
	}

	@Test
	public void testIterEmpty() throws Exception {
		DBTestRunner r = new DBTestRunner();
		try {
			Iterator0 iter = r.db.newIterator(new ReadOptions());

			iter.seekToFirst();
			assertEquals(r.iterStatus(iter), "(invalid)");

			iter.seekToLast();
			assertEquals(r.iterStatus(iter), "(invalid)");

			iter.seek(new DefaultSlice("foo"));
			assertEquals(r.iterStatus(iter), "(invalid)");

			iter.delete();
		} finally {
			r.delete();
		}
	}

	@Test
	public void testIterSingle() throws Exception {
		DBTestRunner r = new DBTestRunner();
		try {
			assertTrue(r.put("a", "va").ok());
			Iterator0 iter = r.db.newIterator(new ReadOptions());

			iter.seekToFirst();
			assertEquals(r.iterStatus(iter), "a->va");
			iter.next();
			assertEquals(r.iterStatus(iter), "(invalid)");
			iter.seekToFirst();
			assertEquals(r.iterStatus(iter), "a->va");
			iter.prev();
			assertEquals(r.iterStatus(iter), "(invalid)");

			iter.seekToLast();
			assertEquals(r.iterStatus(iter), "a->va");
			iter.next();
			assertEquals(r.iterStatus(iter), "(invalid)");
			iter.seekToLast();
			assertEquals(r.iterStatus(iter), "a->va");
			iter.prev();
			assertEquals(r.iterStatus(iter), "(invalid)");

			iter.seek(new DefaultSlice(""));
			assertEquals(r.iterStatus(iter), "a->va");
			iter.next();
			assertEquals(r.iterStatus(iter), "(invalid)");

			iter.seek(new DefaultSlice("a"));
			assertEquals(r.iterStatus(iter), "a->va");
			iter.next();
			assertEquals(r.iterStatus(iter), "(invalid)");

			iter.seek(new DefaultSlice("b"));
			assertEquals(r.iterStatus(iter), "(invalid)");

			iter.delete();
		} finally {
			r.delete();
		}
	}

	@Test
	public void testIterMulti() throws Exception {

		DBTestRunner r = new DBTestRunner();
		try {
			assertTrue(r.put("a", "va").ok());
			assertTrue(r.put("b", "vb").ok());
			assertTrue(r.put("c", "vc").ok());
			Iterator0 iter = r.db.newIterator(new ReadOptions());

			iter.seekToFirst();
			assertEquals(r.iterStatus(iter), "a->va");
			iter.next();
			assertEquals(r.iterStatus(iter), "b->vb");
			iter.next();
			assertEquals(r.iterStatus(iter), "c->vc");
			iter.next();
			assertEquals(r.iterStatus(iter), "(invalid)");
			iter.seekToFirst();
			assertEquals(r.iterStatus(iter), "a->va");
			iter.prev();
			assertEquals(r.iterStatus(iter), "(invalid)");

			iter.seekToLast();
			assertEquals(r.iterStatus(iter), "c->vc");
			iter.prev();
			assertEquals(r.iterStatus(iter), "b->vb");
			iter.prev();
			assertEquals(r.iterStatus(iter), "a->va");
			iter.prev();
			assertEquals(r.iterStatus(iter), "(invalid)");
			iter.seekToLast();
			assertEquals(r.iterStatus(iter), "c->vc");
			iter.next();
			assertEquals(r.iterStatus(iter), "(invalid)");

			iter.seek(new DefaultSlice(""));
			assertEquals(r.iterStatus(iter), "a->va");
			iter.seek(new DefaultSlice("a"));
			assertEquals(r.iterStatus(iter), "a->va");
			iter.seek(new DefaultSlice("ax"));
			assertEquals(r.iterStatus(iter), "b->vb");
			iter.seek(new DefaultSlice("b"));
			assertEquals(r.iterStatus(iter), "b->vb");
			iter.seek(new DefaultSlice("z"));
			assertEquals(r.iterStatus(iter), "(invalid)");

			// Switch from reverse to forward
			iter.seekToLast();
			iter.prev();
			iter.prev();
			iter.next();
			assertEquals(r.iterStatus(iter), "b->vb");

			// Switch from forward to reverse
			iter.seekToFirst();
			iter.next();
			iter.next();
			iter.prev();
			assertEquals(r.iterStatus(iter), "b->vb");

			// Make sure iter stays at snapshot
			assertTrue(r.put("a", "va2").ok());
			assertTrue(r.put("a2", "va3").ok());
			assertTrue(r.put("b", "vb2").ok());
			assertTrue(r.put("c", "vc2").ok());
			assertTrue(r.delete(new DefaultSlice("b")).ok());
			iter.seekToFirst();
			assertEquals(r.iterStatus(iter), "a->va");
			iter.next();
			assertEquals(r.iterStatus(iter), "b->vb");
			iter.next();
			assertEquals(r.iterStatus(iter), "c->vc");
			iter.next();
			assertEquals(r.iterStatus(iter), "(invalid)");
			iter.seekToLast();
			assertEquals(r.iterStatus(iter), "c->vc");
			iter.prev();
			assertEquals(r.iterStatus(iter), "b->vb");
			iter.prev();
			assertEquals(r.iterStatus(iter), "a->va");
			iter.prev();
			assertEquals(r.iterStatus(iter), "(invalid)");

			iter.delete();
		} finally {
			r.delete();
		}
	}

	@Test
	public void testIterSmallAndLargeMix() throws Exception {
		DBTestRunner r = new DBTestRunner();
		try {
			assertTrue(r.put("a", "va").ok());
			assertTrue(r.put("b", TestUtil.makeString(100000, 'b')).ok());
			assertTrue(r.put("c", "vc").ok());
			assertTrue(r.put("d", TestUtil.makeString(100000, 'd')).ok());
			assertTrue(r.put("e", TestUtil.makeString(100000, 'e')).ok());

			Iterator0 iter = r.db.newIterator(new ReadOptions());

			iter.seekToFirst();
			assertEquals(r.iterStatus(iter), "a->va");
			iter.next();
			assertEquals(r.iterStatus(iter), "b->" + TestUtil.makeString(100000, 'b'));
			iter.next();
			assertEquals(r.iterStatus(iter), "c->vc");
			iter.next();
			assertEquals(r.iterStatus(iter), "d->" + TestUtil.makeString(100000, 'd'));
			iter.next();
			assertEquals(r.iterStatus(iter), "e->" + TestUtil.makeString(100000, 'e'));
			iter.next();
			assertEquals(r.iterStatus(iter), "(invalid)");

			iter.seekToLast();
			assertEquals(r.iterStatus(iter), "e->" + TestUtil.makeString(100000, 'e'));
			iter.prev();
			assertEquals(r.iterStatus(iter), "d->" + TestUtil.makeString(100000, 'd'));
			iter.prev();
			assertEquals(r.iterStatus(iter), "c->vc");
			iter.prev();
			assertEquals(r.iterStatus(iter), "b->" + TestUtil.makeString(100000, 'b'));
			iter.prev();
			assertEquals(r.iterStatus(iter), "a->va");
			iter.prev();
			assertEquals(r.iterStatus(iter), "(invalid)");

			iter.delete();
		} finally {
			r.delete();
		}
	}

	@Test
	public void testIterMultiWithDelete() throws Exception {
		DBTestRunner r = new DBTestRunner();
		try {
			do {
				assertTrue(r.put("a", "va").ok());
				assertTrue(r.put("b", "vb").ok());
				assertTrue(r.put("c", "vc").ok());
				assertTrue(r.delete(new DefaultSlice("b")).ok());
				assertEquals("NOT_FOUND", r.get("b"));

				Iterator0 iter = r.db.newIterator(new ReadOptions());
				iter.seek(new DefaultSlice("c"));
				assertEquals(r.iterStatus(iter), "c->vc");
				iter.prev();
				assertEquals(r.iterStatus(iter), "a->va");
				iter.delete();
			} while (r.changeOptions());
		} finally {
			r.delete();
		}
	}

	@Test
	public void testRecover() throws Exception {
		DBTestRunner r = new DBTestRunner();
		try {
			do {
				assertTrue(r.put("foo", "v1").ok());
				assertTrue(r.put("baz", "v5").ok());

				r.reopen();
				assertEquals("v1", r.get("foo"));

				assertEquals("v1", r.get("foo"));
				assertEquals("v5", r.get("baz"));
				assertTrue(r.put("bar", "v2").ok());
				assertTrue(r.put("foo", "v3").ok());

				r.reopen();
				assertEquals("v3", r.get("foo"));
				assertTrue(r.put("foo", "v4").ok());
				assertEquals("v4", r.get("foo"));
				assertEquals("v2", r.get("bar"));
				assertEquals("v5", r.get("baz"));
			} while (r.changeOptions());
		} finally {
			r.delete();
		}
	}

	@Test
	public void testRecoveryWithEmptyLog() throws Exception {
		DBTestRunner r = new DBTestRunner();
		try {
			do {
				assertTrue(r.put("foo", "v1").ok());
				assertTrue(r.put("foo", "v2").ok());
				r.reopen();
				r.reopen();
				assertTrue(r.put("foo", "v3").ok());
				r.reopen();
				assertEquals("v3", r.get("foo"));
			} while (r.changeOptions());
		} finally {
			r.delete();
		}
	}

	@Test
	public void testRecoverDuringMemtableCompaction() throws Exception {
		DBTestRunner r = new DBTestRunner();
		try {
			do {
				Options options = r.currentOptions().cloneOptions();
				options.env = r.env;
				options.writeBufferSize = 1000000;
				r.reopen(options);

				// Trigger a long memtable compaction and reopen the database during it
				assertTrue(r.put("foo", "v1").ok()); // Goes to 1st log file
				assertTrue(r.put("big1", TestUtil.makeString(10000000, 'x')).ok()); // Fills memtable
				assertTrue(r.put("big2", TestUtil.makeString(1000, 'y')).ok()); // Triggers compaction
				assertTrue(r.put("bar", "v2").ok()); // Goes to new log file

				r.reopen(options);
				assertEquals("v1", r.get("foo"));
				assertEquals("v2", r.get("bar"));
				assertEquals(TestUtil.makeString(10000000, 'x'), r.get("big1"));
				assertEquals(TestUtil.makeString(1000, 'y'), r.get("big2"));

				assertEquals("v2", r.get("bar"));
				assertEquals(TestUtil.makeString(10000000, 'x'), r.get("big1"));

				assertEquals(TestUtil.makeString(1000, 'y'), r.get("big2"));
				assertEquals("v1", r.get("foo"));

			} while (r.changeOptions());
		} finally {
			r.delete();
		}
	}

	static String Key(int i) {
		return String.format("key%06d", i);
	}

	@Test
	public void testMinorCompactionsHappen() throws Exception {

		DBTestRunner r = new DBTestRunner();
		try {
			Options options = r.currentOptions().cloneOptions();
			options.writeBufferSize = 10000;
			r.reopen(options);

			final int N = 500;

			int starting_num_tables = r.totalTableFiles();
			for (int i = 0; i < N; i++) {
				assertTrue(r.put(Key(i), Key(i) + TestUtil.makeString(1000, 'v')).ok());
			}

			int ending_num_tables = r.totalTableFiles();
			assertTrue(ending_num_tables > starting_num_tables);

			for (int i = 0; i < N; i++) {
				assertEquals(Key(i) + TestUtil.makeString(1000, 'v'), r.get(Key(i)));
			}

			Options options2 = r.currentOptions().cloneOptions();
			options2.reuseLogs = true;
			r.reopen(options2);

			for (int i = 0; i < N; i++) {
				assertEquals(Key(i) + TestUtil.makeString(1000, 'v'), r.get(Key(i)));
			}
		} finally {
			r.delete();
		}
	}

	@Test
	public void testRecoverWithLargeLog() throws Exception {

		DBTestRunner r = new DBTestRunner();
		try {
			{
				Options options = r.currentOptions().cloneOptions();
				r.reopen(options);
				assertTrue(r.put("big1", TestUtil.makeString(200000, '1')).ok());
				assertTrue(r.put("big2", TestUtil.makeString(200000, '2')).ok());
				assertTrue(r.put("small3", TestUtil.makeString(10, '3')).ok());
				assertTrue(r.put("small4", TestUtil.makeString(10, '4')).ok());
				assertEquals(r.numTableFilesAtLevel(0), 0);
			}

			// Make sure that if we re-open with a small write buffer size that
			// we flush table files in the middle of a large log file.
			Options options = r.currentOptions().cloneOptions();
			options.writeBufferSize = 100000;
			r.reopen(options);
			assertEquals(r.numTableFilesAtLevel(0), 3);
			assertEquals(TestUtil.makeString(200000, '1'), r.get("big1"));
			assertEquals(TestUtil.makeString(200000, '2'), r.get("big2"));
			assertEquals(TestUtil.makeString(10, '3'), r.get("small3"));
			assertEquals(TestUtil.makeString(10, '4'), r.get("small4"));
			assertTrue(r.numTableFilesAtLevel(0) > 1);
		} finally {
			r.delete();
		}
	}

	@Test
	public void testCompactionsGenerateMultipleFiles() throws Exception {

		DBTestRunner r = new DBTestRunner();
		try {
			Options options = r.currentOptions().cloneOptions();
			options.writeBufferSize = 100000000; // Large write buffer
			r.reopen(options);

			Random0 rnd = new Random0(301);

			// Write 8MB (80 values, each 100K)
			assertEquals(r.numTableFilesAtLevel(0), 0);
			ArrayList<ByteBuf> values = new ArrayList<ByteBuf>();
			for (int i = 0; i < 80; i++) {
				values.add(randomString(rnd, 100000));
				assertTrue(r.put(Key(i), values.get(i).encodeToString()).ok());
			}

			// Reopening moves updates to level-0
			r.reopen(options);
			r.dbfull().TEST_CompactRange(0, null, null);

			assertEquals(r.numTableFilesAtLevel(0), 0);
			assertTrue(r.numTableFilesAtLevel(1) > 1);
			for (int i = 0; i < 80; i++) {
				assertEquals(r.get(Key(i)), values.get(i).encodeToString());
			}
		} finally {
			r.delete();
		}
	}

	@Test
	public void testRepeatedWritesToSameKey() throws Exception {

		DBTestRunner r = new DBTestRunner();
		try {
			Options options = r.currentOptions().cloneOptions();
			options.env = r.env;
			options.writeBufferSize = 100000; // Small write buffer
			r.reopen(options);

			// We must have at most one file per level except for level-0,
			// which may have up to kL0_StopWritesTrigger files.
			final int kMaxFiles = DBFormat.kNumLevels + DBFormat.kL0_StopWritesTrigger;

			Random0 rnd = new Random0(301);
			ByteBuf value = randomString(rnd, 2 * options.writeBufferSize);

			System.out.println("[DEBUG] ============= value.size=" + value.size());

			for (int i = 0; i < 5 * kMaxFiles; i++) {
				r.put("key", value.encodeToString());
				assertTrue(r.totalTableFiles() < kMaxFiles);
				System.err.printf("========================================\nafter %d: %d files\n========================================\n", i + 1, r.totalTableFiles());
			}
		} finally {
			r.delete();
		}
	}

	@Test
	public void testSparseMerge() throws Exception {

		DBTestRunner r = new DBTestRunner();
		try {
			Options options = r.currentOptions().cloneOptions();
			options.compression = CompressionType.kNoCompression;
			r.reopen(options);

			r.fillLevels("A", "Z");

			// Suppose there is:
			// small amount of data with prefix A
			// large amount of data with prefix B
			// small amount of data with prefix C
			// and that recent updates have made small changes to all three
			// prefixes.
			// Check that we do not do a compaction that merges all of B in one
			// shot.
			String value = TestUtil.makeString(1000, 'x');
			r.put("A", "va");
			// Write approximately 100MB of "B" values
			for (int i = 0; i < 100000; i++) {
				String key = String.format("B%010d", i);
				r.put(key, value);
			}
			r.put("C", "vc");
			r.dbfull().TEST_CompactMemTable();
			r.dbfull().TEST_CompactRange(0, null, null);

			// Make sparse update
			r.put("A", "va2");
			r.put("B100", "bvalue2");
			r.put("C", "vc2");
			r.dbfull().TEST_CompactMemTable();

			// Compactions should not cause us to create a situation where
			// a file overlaps too much data at the next level.
			assertTrue(r.dbfull().TEST_MaxNextLevelOverlappingBytes() <= 20 * 1048576);
			r.dbfull().TEST_CompactRange(0, null, null);
			assertTrue(r.dbfull().TEST_MaxNextLevelOverlappingBytes() <= 20 * 1048576);
			r.dbfull().TEST_CompactRange(1, null, null);
			assertTrue(r.dbfull().TEST_MaxNextLevelOverlappingBytes() <= 20 * 1048576);
		} finally {
			r.delete();
		}
	}

	static boolean between(long val, long low, long high) {
		boolean result = (val >= low) && (val <= high);
		if (!result) {
			System.err.printf("Value %d is not in range [%d, %d]\n", val, low, high);
		}
		return result;
	}

	Slice slice(String s) {
		return new DefaultSlice(s);
	}

	@Test
	public void testApproximateSizes() throws Exception {

		DBTestRunner r = new DBTestRunner();
		try {
			do {
				Options options = r.currentOptions().cloneOptions();
				options.writeBufferSize = 100000000; // Large write buffer
				options.compression = CompressionType.kNoCompression;
				r.destroyAndReopen();

				assertTrue(between(r.size(slice(""), slice("xyz")), 0, 0));
				r.reopen(options);
				assertTrue(between(r.size(slice(""), slice("xyz")), 0, 0));

				// Write 8MB (80 values, each 100K)
				assertEquals(r.numTableFilesAtLevel(0), 0);
				final int N = 80;
				final int S1 = 100000;
				final int S2 = 105000; // Allow some expansion from metadata
				Random0 rnd = new Random0(301);
				for (int i = 0; i < N; i++) {
					assertTrue(r.put(Key(i), randomString(rnd, S1)).ok());
				}

				// 0 because GetApproximateSizes() does not account for memtable
				// space
				assertTrue(between(r.size(slice(""), slice(Key(50))), 0, 0));

				if (options.reuseLogs) {
					// Recovery will reuse memtable, and GetApproximateSizes() does
					// not
					// account for memtable usage;
					r.reopen(options);
					assertTrue(between(r.size(slice(""), slice(Key(50))), 0, 0));
					continue;
				}

				// Check sizes across recovery by reopening a few times
				for (int run = 0; run < 3; run++) {
					r.reopen(options);

					for (int compact_start = 0; compact_start < N; compact_start += 10) {
						for (int i = 0; i < N; i += 10) {
							assertTrue(between(r.size(slice(""), slice(Key(i))), S1 * i, S2 * i));
							assertTrue(between(r.size(slice(""), slice(Key(i) + ".suffix")), S1 * (i + 1), S2 * (i + 1)));
							assertTrue(between(r.size(slice(Key(i)), slice(Key(i + 10))), S1 * 10, S2 * 10));
						}
						assertTrue(between(r.size(slice(""), slice(Key(50))), S1 * 50, S2 * 50));
						assertTrue(between(r.size(slice(""), slice(Key(50) + ".suffix")), S1 * 50, S2 * 50));

						String cstart_str = Key(compact_start);
						String cend_str = Key(compact_start + 9);
						Slice cstart = slice(cstart_str);
						Slice cend = slice(cend_str);
						r.dbfull().TEST_CompactRange(0, cstart, cend);
					}

					assertEquals(r.numTableFilesAtLevel(0), 0);
					assertTrue(r.numTableFilesAtLevel(1) > 0);
				}
			} while (r.changeOptions());
		} finally {
			r.delete();
		}
	}

	@Test
	public void testApproximateSizes_MixOfSmallAndLarge() throws Exception {

		DBTestRunner r = new DBTestRunner();
		try {
			do {
				Options options = r.currentOptions().cloneOptions();
				options.compression = CompressionType.kNoCompression;
				r.reopen();

				Random0 rnd = new Random0(301);
				ByteBuf big1 = randomString(rnd, 100000);
				assertTrue(r.put(Key(0), randomString(rnd, 10000)).ok());
				assertTrue(r.put(Key(1), randomString(rnd, 10000)).ok());
				assertTrue(r.put(Key(2), big1).ok());
				assertTrue(r.put(Key(3), randomString(rnd, 10000)).ok());
				assertTrue(r.put(Key(4), big1).ok());
				assertTrue(r.put(Key(5), randomString(rnd, 10000)).ok());
				assertTrue(r.put(Key(6), randomString(rnd, 300000)).ok());
				assertTrue(r.put(Key(7), randomString(rnd, 10000)).ok());

				if (options.reuseLogs) {
					// Need to force a memtable compaction since recovery does not
					// do so.
					assertTrue(r.dbfull().TEST_CompactMemTable().ok());
				}

				// Check sizes across recovery by reopening a few times
				for (int run = 0; run < 3; run++) {
					r.reopen(options);

					assertTrue(between(r.size(slice(""), slice(Key(0))), 0, 0));
					assertTrue(between(r.size(slice(""), slice(Key(1))), 10000, 11000));
					assertTrue(between(r.size(slice(""), slice(Key(2))), 20000, 21000));
					assertTrue(between(r.size(slice(""), slice(Key(3))), 120000, 121000));
					assertTrue(between(r.size(slice(""), slice(Key(4))), 130000, 131000));
					assertTrue(between(r.size(slice(""), slice(Key(5))), 230000, 231000));
					assertTrue(between(r.size(slice(""), slice(Key(6))), 240000, 241000));
					assertTrue(between(r.size(slice(""), slice(Key(7))), 540000, 541000));
					assertTrue(between(r.size(slice(""), slice(Key(8))), 550000, 560000));

					assertTrue(between(r.size(slice(Key(3)), slice(Key(5))), 110000, 111000));

					r.dbfull().TEST_CompactRange(0, null, null);
				}
			} while (r.changeOptions());
		} finally {
			r.delete();
		}
	}

	@Test
	public void testIteratorPinsRef() throws Exception {

		DBTestRunner r = new DBTestRunner();
		try {
			r.put("foo", "hello");

			// Get iterator that will yield the current contents of the DB.
			Iterator0 iter = r.db.newIterator(new ReadOptions());

			// Write to force compactions
			r.put("foo", "newvalue1");
			for (int i = 0; i < 100; i++) {
				assertTrue(r.put(Key(i), Key(i) + TestUtil.makeString(100000, 'v')).ok()); // 100K values
			}
			r.put("foo", "newvalue2");

			iter.seekToFirst();
			assertTrue(iter.valid());
			assertEquals("foo", iter.key().encodeToString());
			assertEquals("hello", iter.value().encodeToString());
			iter.next();
			assertTrue(!iter.valid());
			iter.delete();
		} finally {
			r.delete();
		}
	}

	@Test
	public void testSnapshot() throws Exception {

		DBTestRunner r = new DBTestRunner();
		try {
			do {
				r.put("foo", "v1");
				final Snapshot s1 = r.db.getSnapshot();
				r.put("foo", "v2");
				final Snapshot s2 = r.db.getSnapshot();
				r.put("foo", "v3");
				final Snapshot s3 = r.db.getSnapshot();

				r.put("foo", "v4");
				assertEquals("v1", r.get("foo", s1));
				assertEquals("v2", r.get("foo", s2));
				assertEquals("v3", r.get("foo", s3));
				assertEquals("v4", r.get("foo"));

				r.db.releaseSnapshot(s3);
				assertEquals("v1", r.get("foo", s1));
				assertEquals("v2", r.get("foo", s2));
				assertEquals("v4", r.get("foo"));

				r.db.releaseSnapshot(s1);
				assertEquals("v2", r.get("foo", s2));
				assertEquals("v4", r.get("foo"));

				r.db.releaseSnapshot(s2);
				assertEquals("v4", r.get("foo"));
			} while (r.changeOptions());
		} finally {
			r.delete();
		}
	}

	@Test
	public void testHiddenValuesAreRemoved() throws Exception {

		DBTestRunner r = new DBTestRunner();
		try {
			do {
				Random0 rnd = new Random0(301);
				r.fillLevels("a", "z");

				ByteBuf big = randomString(rnd, 50000);
				r.put("foo", big);
				r.put("pastfoo", "v");
				final Snapshot snapshot = r.db.getSnapshot();
				r.put("foo", "tiny");
				r.put("pastfoo2", "v2"); // Advance sequence number one more

				assertTrue(r.dbfull().TEST_CompactMemTable().ok());
				assertTrue(r.numTableFilesAtLevel(0) > 0);

				assertEquals(big.encodeToString(), r.get("foo", snapshot));
				assertTrue(between(r.size(slice(""), slice("pastfoo")), 50000, 60000));

				r.db.releaseSnapshot(snapshot);
				assertEquals(r.allEntriesFor(slice("foo")), "[ tiny, " + big.encodeToString() + " ]");

				Slice x = slice("x");
				r.dbfull().TEST_CompactRange(0, null, x);
				assertEquals(r.allEntriesFor(slice("foo")), "[ tiny ]");
				assertEquals(r.numTableFilesAtLevel(0), 0);
				assertTrue(r.numTableFilesAtLevel(1) >= 1);

				r.dbfull().TEST_CompactRange(1, null, x);
				assertEquals(r.allEntriesFor(slice("foo")), "[ tiny ]");
				assertTrue(between(r.size(slice(""), slice("pastfoo")), 0, 1000));
			} while (r.changeOptions());
		} finally {
			r.delete();
		}
	}

	@Test
	public void testDeletionMarkers1() throws Exception {

		DBTestRunner r = new DBTestRunner();
		try {
			r.put("foo", "v1");

			assertTrue(r.dbfull().TEST_CompactMemTable().ok());

			final int last = DBFormat.kMaxMemCompactLevel;

			assertEquals(r.numTableFilesAtLevel(last), 1); // foo => v1 is now in last level

			// Place a table at level last-1 to prevent merging with preceding mutation
			r.put("a", "begin");
			r.put("z", "end");
			r.dbfull().TEST_CompactMemTable();
			assertEquals(r.numTableFilesAtLevel(last), 1);
			assertEquals(r.numTableFilesAtLevel(last - 1), 1);

			r.delete("foo");
			r.put("foo", "v2");
			assertEquals(r.allEntriesFor(slice("foo")), "[ v2, DEL, v1 ]");
			assertTrue(r.dbfull().TEST_CompactMemTable().ok()); // Moves to level last-2

			assertEquals(r.allEntriesFor(slice("foo")), "[ v2, DEL, v1 ]");
			Slice z = slice("z");
			r.dbfull().TEST_CompactRange(last - 2, null, z);

			// DEL eliminated, but v1 remains because we aren't compacting that level
			// (DEL can be eliminated because v2 hides v1).
			assertEquals(r.allEntriesFor(slice("foo")), "[ v2, v1 ]");
			r.dbfull().TEST_CompactRange(last - 1, null, null);

			// Merging last-1 w/ last, so we are the base level for "foo", so DEL is
			// removed. (as is v1).
			assertEquals(r.allEntriesFor(slice("foo")), "[ v2 ]");
		} finally {
			r.delete();
		}
	}

	@Test
	public void testDeletionMarkers2() throws Exception {

		DBTestRunner r = new DBTestRunner();
		try {
			r.put("foo", "v1");
			assertTrue(r.dbfull().TEST_CompactMemTable().ok());
			final int last = DBFormat.kMaxMemCompactLevel;
			assertEquals(r.numTableFilesAtLevel(last), 1); // foo => v1 is now in last level

			// Place a table at level last-1 to prevent merging with preceding
			// mutation
			r.put("a", "begin");
			r.put("z", "end");
			r.dbfull().TEST_CompactMemTable();
			assertEquals(r.numTableFilesAtLevel(last), 1);
			assertEquals(r.numTableFilesAtLevel(last - 1), 1);

			r.delete("foo");
			assertEquals(r.allEntriesFor(slice("foo")), "[ DEL, v1 ]");
			assertTrue(r.dbfull().TEST_CompactMemTable().ok()); // Moves to level
																// last-2
			assertEquals(r.allEntriesFor(slice("foo")), "[ DEL, v1 ]");
			r.dbfull().TEST_CompactRange(last - 2, null, null);
			// DEL kept: "last" file overlaps
			assertEquals(r.allEntriesFor(slice("foo")), "[ DEL, v1 ]");
			r.dbfull().TEST_CompactRange(last - 1, null, null);
			// Merging last-1 w/ last, so we are the base level for "foo", so
			// DEL is removed. (as is v1).
			assertEquals(r.allEntriesFor(slice("foo")), "[ ]");
		} finally {
			r.delete();
		}
	}

	@Test
	public void testOverlapInLevel0() throws Exception {

		DBTestRunner r = new DBTestRunner();
		try {
			do {
				assertEquals(DBFormat.kMaxMemCompactLevel, 2);

				// Fill levels 1 and 2 to disable the pushing of new memtables to
				// levels > 0.
				assertTrue(r.put("100", "v100").ok());
				assertTrue(r.put("999", "v999").ok());
				r.dbfull().TEST_CompactMemTable();
				assertTrue(r.delete("100").ok());
				assertTrue(r.delete("999").ok());
				r.dbfull().TEST_CompactMemTable();
				assertEquals("0,1,1", r.filesPerLevel());

				// Make files spanning the following ranges in level-0:
				// files[0] 200 .. 900
				// files[1] 300 .. 500
				// Note that files are sorted by smallest key.
				assertTrue(r.put("300", "v300").ok());
				assertTrue(r.put("500", "v500").ok());
				r.dbfull().TEST_CompactMemTable();
				assertTrue(r.put("200", "v200").ok());
				assertTrue(r.put("600", "v600").ok());
				assertTrue(r.put("900", "v900").ok());
				r.dbfull().TEST_CompactMemTable();
				assertEquals("2,1,1", r.filesPerLevel());

				// Compact away the placeholder files we created initially
				r.dbfull().TEST_CompactRange(1, null, null);
				r.dbfull().TEST_CompactRange(2, null, null);
				assertEquals("2", r.filesPerLevel());

				// Do a memtable compaction. Before bug-fix, the compaction would
				// not detect the overlap with level-0 files and would incorrectly
				// place
				// the deletion in a deeper level.
				assertTrue(r.delete("600").ok());
				r.dbfull().TEST_CompactMemTable();
				assertEquals("3", r.filesPerLevel());
				assertEquals("NOT_FOUND", r.get("600"));
			} while (r.changeOptions());
		} finally {
			r.delete();
		}
	}

	@Test
	public void testL0_CompactionBug_Issue44_a() throws Exception {

		DBTestRunner r = new DBTestRunner();
		try {
			r.reopen();
			assertTrue(r.put("b", "v").ok());
			r.reopen();
			assertTrue(r.delete("b").ok());
			assertTrue(r.delete("a").ok());
			r.reopen();
			assertTrue(r.delete("a").ok());
			r.reopen();
			assertTrue(r.put("a", "v").ok());
			r.reopen();
			r.reopen();
			assertEquals("(a->v)", r.contents());
			delayMilliseconds(1000); // Wait for compaction to finish
			assertEquals("(a->v)", r.contents());
		} finally {
			r.delete();
		}
	}

	@Test
	public void testL0_CompactionBug_Issue44_b() throws Exception {

		DBTestRunner r = new DBTestRunner();
		try {
			r.reopen();
			r.put("", "");
			r.reopen();
			r.delete("e");
			r.put("", "");
			r.reopen();
			r.put("c", "cv");
			r.reopen();
			r.put("", "");
			r.reopen();
			r.put("", "");
			delayMilliseconds(1000); // Wait for compaction to finish
			r.reopen();
			r.put("d", "dv");
			r.reopen();
			r.put("", "");
			r.reopen();
			r.delete("d");
			r.delete("b");
			r.reopen();
			assertEquals("(->)(c->cv)", r.contents());
			delayMilliseconds(1000); // Wait for compaction to finish
			assertEquals("(->)(c->cv)", r.contents());
		} finally {
			r.delete();
		}
	}

	static class NewComparator extends Comparator0 {
		public String name() {
			return "leveldb.NewComparator";
		}

		public int compare(byte[] a, int aoff, int asize, byte[] b, int boff, int bsize) {
			return BytewiseComparatorImpl.getInstance().compare(a, aoff, asize, b, boff, bsize);
		}

		public void findShortestSeparator(ByteBuf s, Slice l) {
			BytewiseComparatorImpl.getInstance().findShortestSeparator(s, l);
		}

		public void findShortSuccessor(ByteBuf key) {
			BytewiseComparatorImpl.getInstance().findShortSuccessor(key);
		}
	};

	@Test
	public void testComparatorCheck() throws Exception {
		DBTestRunner r = new DBTestRunner();
		try {
			NewComparator cmp = new NewComparator();
			Options newOptions = r.currentOptions().cloneOptions();
			newOptions.comparator = cmp;
			Status s = r.tryReopen(newOptions);
			assertTrue(!s.ok());
			System.out.println("s=" + s);
			assertTrue(s.toString().indexOf("comparator") >= 0);
		} finally {
			r.delete();
		}
	}

	static class NumberComparator extends Comparator0 {
		public String name() {
			return "test.NumberComparator";
		}

		public int compare(byte[] a, int aoff, int asize, byte[] b, int boff, int bsize) {
			return toNumber(new DefaultSlice(a, aoff, asize)) - toNumber(new DefaultSlice(b, boff, bsize));
		}

		public void findShortestSeparator(ByteBuf s, Slice l) {
			toNumber(new DefaultSlice(s)); // Check format
			toNumber(l); // Check format
		}

		public void findShortSuccessor(ByteBuf key) {
			toNumber(new DefaultSlice(key)); // Check format
		}

		static int toNumber(Slice x) {
			// Check that there are no extra characters.
			assertTrue(x.size() >= 2 && x.getByte(0) == (int) ('[' - '\0') && x.getByte(x.size() - 1) == (']' - '\0'));
			String s = x.toString().split("\\[")[1].split("\\]")[0];
			int val = 0;
			if (s.length() > 1 && s.charAt(0) == '0' && s.charAt(1) == 'x') {
				val = Integer.parseInt(s.substring(2), 16);
			} else {
				val = Integer.parseInt(s);
			}
			return val;
		}
	};

	@Test
	public void testCustomComparator() throws Exception {
		DBTestRunner r = new DBTestRunner();
		try {
			NumberComparator cmp = new NumberComparator();
			Options new_options = r.currentOptions().cloneOptions();
			new_options.createIfMissing = true;
			new_options.comparator = cmp;
			new_options.filterPolicy = null; // Cannot use bloom filters
			new_options.writeBufferSize = 1000; // Compact more often
			r.destroyAndReopen(new_options);
			assertTrue(r.put("[10]", "ten").ok());
			assertTrue(r.put("[0x14]", "twenty").ok());
			for (int i = 0; i < 2; i++) {
				assertEquals("ten", r.get("[10]"));
				assertEquals("ten", r.get("[0xa]"));
				assertEquals("twenty", r.get("[20]"));
				assertEquals("twenty", r.get("[0x14]"));
				assertEquals("NOT_FOUND", r.get("[15]"));
				assertEquals("NOT_FOUND", r.get("[0xf]"));
				r.compact("[0]", "[9999]");
			}

			for (int run = 0; run < 2; run++) {
				for (int i = 0; i < 1000; i++) {
					String s = String.format("[%d]", i * 10);
					assertTrue(r.put(s, s).ok());
				}
				r.compact("[0]", "[1000000]");
			}

		} finally {
			r.delete();
		}
	}

	@Test
	public void testManualCompaction() throws Exception {
		DBTestRunner r = new DBTestRunner();
		try {
			assertEquals(DBFormat.kMaxMemCompactLevel, 2);

			r.makeTables(3, "p", "q");
			assertEquals("1,1,1", r.filesPerLevel());

			// Compaction range falls before files
			r.compact("", "c");
			assertEquals("1,1,1", r.filesPerLevel());

			// Compaction range falls after files
			r.compact("r", "z");
			assertEquals("1,1,1", r.filesPerLevel());

			// Compaction range overlaps files
			r.compact("p1", "p9");
			assertEquals("0,0,1", r.filesPerLevel());

			// Populate a different range
			r.makeTables(3, "c", "e");
			assertEquals("1,1,2", r.filesPerLevel());

			// Compact just the new range
			r.compact("b", "f");
			assertEquals("0,0,2", r.filesPerLevel());

			// Compact all
			r.makeTables(1, "a", "z");
			assertEquals("0,1,2", r.filesPerLevel());
			r.db.compactRange(null, null);
			assertEquals("0,0,1", r.filesPerLevel());

		} finally {
			r.delete();
		}
	}

	@Test
	public void testDBOpen_Options() throws Exception {
		DBTestRunner r = new DBTestRunner();
		try {
			String dbname = TestUtil.tmpDir() + "/db_options_test";
			LevelDB.destroyDB(dbname, new Options());

			// Does not exist, and create_if_missing == false: error
			Object0<DB> db0 = new Object0<>();
			Options opts = new Options();
			opts.createIfMissing = false;
			Status s = LevelDB.newDB(opts, dbname, db0);
			DB db = db0.getValue();
			assertTrue(s.toString().indexOf("does not exist") >= 0);
			assertTrue(db == null);

			// Does not exist, and create_if_missing == true: OK
			opts.createIfMissing = true;
			db0.setValue(null);
			s = LevelDB.newDB(opts, dbname, db0);
			db = db0.getValue();
			assertTrue(s.ok());
			assertTrue(db != null);

			db.close();
			db = null;

			// Does exist, and error_if_exists == true: error
			opts.createIfMissing = false;
			opts.errorIfExists = true;
			db0.setValue(null);
			s = LevelDB.newDB(opts, dbname, db0);
			db = db0.getValue();
			assertTrue(s.toString().indexOf("exists") >= 0);
			assertTrue(db == null);

			// Does exist, and error_if_exists == false: OK
			opts.createIfMissing = true;
			opts.errorIfExists = false;
			db0.setValue(null);
			s = LevelDB.newDB(opts, dbname, db0);
			db = db0.getValue();
			assertTrue(s.ok());
			assertTrue(db != null);

			db.close();

			db = null;

		} finally {
			r.delete();
		}
	}

	@Test
	public void testLocking() throws Exception {
		DBTestRunner r = new DBTestRunner();
		try {
			Object0<DB> db2 = new Object0<>();
			Status s = LevelDB.newDB(r.currentOptions().cloneOptions(), r.dbname, db2);
			assertTrue(!s.ok());

		} finally {
			r.delete();
		}
	}

	// Check that number of files does not grow when we are out of space
	@Test
	public void testNoSpace() throws Exception {
		DBTestRunner r = new DBTestRunner();
		try {
			Options options = r.currentOptions().cloneOptions();
			options.env = r.env;
			r.reopen(options);

			assertTrue(r.put("foo", "v1").ok());
			assertEquals("v1", r.get("foo"));
			r.compact("a", "z");
			final int num_files = r.countFiles();
			r.env.noSpace.set(r.env); // Force out-of-space errors
			for (int i = 0; i < 10; i++) {
				for (int level = 0; level < DBFormat.kNumLevels - 1; level++) {
					r.dbfull().TEST_CompactRange(level, null, null);
				}
			}
			r.env.noSpace.set(null);
			assertTrue(r.countFiles() < num_files + 3);

		} finally {
			r.delete();
		}
	}

	@Test
	public void testNonWritableFileSystem() throws Exception {
		DBTestRunner r = new DBTestRunner();
		try {
			Options options = r.currentOptions().cloneOptions();
			options.writeBufferSize = 1000;
			options.env = r.env;
			r.reopen(options);
			assertTrue(r.put("foo", "v1").ok());
			r.env.nonWritable.set(r.env); // Force errors for new files
			String big = TestUtil.makeString(100000, 'x');
			int errors = 0;
			for (int i = 0; i < 20; i++) {
				System.err.printf("iter %d; errors %d\n", i, errors);
				if (!r.put("foo", big).ok()) {
					errors++;
					delayMilliseconds(100);
				}
			}
			assertTrue(errors > 0);
			r.env.nonWritable.set(null);

		} finally {
			r.delete();
		}
	}

	@Test
	public void testWriteSyncError() throws Exception {
		DBTestRunner r = new DBTestRunner();
		try {
			// Check that log sync errors cause the DB to disallow future writes.

			// (a) Cause log sync calls to fail
			Options options = r.currentOptions().cloneOptions();
			options.env = r.env;
			r.reopen(options);
			r.env.dataSyncError.set(r.env);

			// (b) Normal write should succeed
			WriteOptions w = new WriteOptions();
			assertTrue(r.db.put(w, new DefaultSlice("k1"), new DefaultSlice("v1")).ok());
			assertEquals("v1", r.get("k1"));

			// (c) Do a sync write; should fail
			w.sync = true;
			assertTrue(!r.db.put(w, new DefaultSlice("k2"), new DefaultSlice("v2")).ok());
			assertEquals("v1", r.get("k1"));
			assertEquals("NOT_FOUND", r.get("k2"));

			// (d) make sync behave normally
			r.env.dataSyncError.set(null);

			// (e) Do a non-sync write; should fail
			w.sync = false;
			assertTrue(!r.db.put(w, new DefaultSlice("k3"), new DefaultSlice("v3")).ok());
			assertEquals("v1", r.get("k1"));
			assertEquals("NOT_FOUND", r.get("k2"));
			assertEquals("NOT_FOUND", r.get("k3"));

		} finally {
			r.delete();
		}
	}

	@Test
	public void testManifestWriteError() throws Exception {
		DBTestRunner r = new DBTestRunner();
		try {
			// Test for the following problem:
			// (a) Compaction produces file F
			// (b) Log record containing F is written to MANIFEST file, but Sync()
			// fails
			// (c) GC deletes F
			// (d) After reopening DB, reads fail since deleted F is named in log
			// record

			// We iterate twice. In the second iteration, everything is the
			// same except the log record never makes it to the MANIFEST file.
			for (int iter = 0; iter < 2; iter++) {
				AtomicReference<Object> error_type = (iter == 0) ? r.env.manifestSyncError : r.env.manifestWriteError;

				// Insert foo=>bar mapping
				Options options = r.currentOptions().cloneOptions();
				options.env = r.env;
				options.createIfMissing = true;
				options.errorIfExists = false;
				r.destroyAndReopen(options);
				assertTrue(r.put("foo", "bar").ok());
				assertEquals("bar", r.get("foo"));

				// Memtable compaction (will succeed)
				r.dbfull().TEST_CompactMemTable();
				assertEquals("bar", r.get("foo"));
				final int last = DBFormat.kMaxMemCompactLevel;
				assertEquals(r.numTableFilesAtLevel(last), 1); // foo=>bar is now in
																// last level

				// Merging compaction (will fail)
				error_type.set(r.env);
				r.dbfull().TEST_CompactRange(last, null, null); // Should fail
				assertEquals("bar", r.get("foo"));

				// Recovery: should not lose data
				error_type.set(null);
				r.reopen(options);
				assertEquals("bar", r.get("foo"));
			}
		} finally {
			r.delete();
		}
	}

	@Test
	public void testMissingSSTFile() throws Exception {
		DBTestRunner r = new DBTestRunner();
		try {
			assertTrue(r.put("foo", "bar").ok());
			assertEquals("bar", r.get("foo"));

			// Dump the memtable to disk.
			r.dbfull().TEST_CompactMemTable();
			assertEquals("bar", r.get("foo"));

			r.close();
			assertTrue(r.deleteAnSSTFile());
			Options options = r.currentOptions().cloneOptions();
			options.paranoidChecks = true;
			Status s = r.tryReopen(options);
			assertTrue(!s.ok());
			assertTrue(s.toString().indexOf("issing") >= 0);

		} finally {
			r.delete();
		}
	}

	@Test
	public void testStillReadSST() throws Exception {
		DBTestRunner r = new DBTestRunner();
		try {
			assertTrue(r.put("foo", "bar").ok());
			assertEquals("bar", r.get("foo"));

			// Dump the memtable to disk.
			r.dbfull().TEST_CompactMemTable();
			assertEquals("bar", r.get("foo"));
			r.close();
			assertTrue(r.renameLDBToSST() > 0);
			Options options = r.currentOptions().cloneOptions();
			options.paranoidChecks = true;
			Status s = r.tryReopen(options);
			assertTrue(s.ok());
			assertEquals("bar", r.get("foo"));
		} finally {
			r.delete();
		}
	}

	@Test
	public void testFilesDeletedAfterCompaction() throws Exception {
		DBTestRunner r = new DBTestRunner();
		try {
			assertTrue(r.put("foo", "v2").ok());
			r.compact("a", "z");
			final int num_files = r.countFiles();

			PrintStream defaultOut = System.out;
			System.setOut(new PrintStream(new DummyOutputStream()));

			for (int i = 0; i < 10; i++) {
				assertTrue(r.put("foo", "v2").ok());
				r.compact("a", "z");
			}

			System.setOut(defaultOut);
			//
			// System.out.printf("DB.dataRange: %s", r.db.debugDataRange());

			assertEquals(r.countFiles(), num_files);

		} finally {
			r.delete();
		}
	}

	@Test
	public void testBloomFilter01() throws Exception {
		DBTestRunner r = new DBTestRunner();
		try {
			r.destroyAndReopen();

			r.env.countRandomReads = true;
			Options options = r.currentOptions().cloneOptions();
			options.env = r.env;
			options.blockCache = Cache.newLRUCache(0); // Prevent cache hits
			options.filterPolicy = BloomFilterPolicy.newBloomFilterPolicy(10);

			r.reopen(options);

			final int N = 1000;
			for (int i = 0; i < N; i++)
				assertTrue(r.put(Key(i), Key(i)).ok());

			System.out.println("\n[DEBUG] TestDB.testBloomFilter compact start");

			r.compact("a", "z");

			for (int i = 0; i < N; i++)
				assertEquals(r.get(Key(i)), Key(i));

		} finally {
			r.delete();
		}
	}

	@Test
	public void testBloomFilter() throws Exception {
		DBTestRunner r = new DBTestRunner();
		try {
			r.destroyAndReopen();

			r.env.countRandomReads = true;
			Options options = r.currentOptions().cloneOptions();
			options.env = r.env;
			options.blockCache = Cache.newLRUCache(0); // Prevent cache hits
			options.filterPolicy = BloomFilterPolicy.newBloomFilterPolicy(10);

			r.reopen(options);

			// Populate multiple layers
			final int N = 10000;
			for (int i = 0; i < N; i++) {
				assertTrue(r.put(Key(i), Key(i)).ok());
			}

			r.compact("a", "z");

			for (int i = 0; i < N; i += 100) {
				assertTrue(r.put(Key(i), Key(i)).ok());
			}

			r.dbfull().TEST_CompactMemTable();

			// Prevent auto compactions triggered by seeks
			r.env.delayDataSync.set(r.env);

			// Lookup present keys. Should rarely read from small sstable.
			r.env.randomReadCounter.set(0);

			for (int i = 0; i < N; i++) {
				assertEquals(Key(i), r.get(Key(i)));
			}

			int reads = (int) r.env.randomReadCounter.get();
			System.err.printf("%d present => %d reads\n", N, reads);
			assertTrue(reads >= N);
			assertTrue(reads <= N + 2 * N / 100);

			// Lookup present keys. Should rarely read from either sstable.
			r.env.randomReadCounter.set(0);
			for (int i = 0; i < N; i++) {
				assertEquals("NOT_FOUND", r.get(Key(i) + ".missing"));
			}
			reads = (int) r.env.randomReadCounter.get();
			System.err.printf("%d missing => %d reads\n", N, reads);
			assertTrue(reads <= 3 * N / 100);

			r.env.delayDataSync.set(null);
			r.close();
			options.blockCache.delete();
			options.filterPolicy.delete();

		} finally {
			r.delete();
		}
	}

	static final int kNumThreads = 4;
	static final int kTestSeconds = 10;
	static final int kNumKeys = 1000;

	static class MTState {
		public DBTestRunner test;
		public AtomicReference<Object> stop = new AtomicReference<>();
		public AtomicLong[] counter;
		public Object[] thread_done;

		public MTState() {
			counter = new AtomicLong[kNumThreads];
			for (int i = 0; i < counter.length; i++)
				counter[i] = new AtomicLong();
			thread_done = new Object[kNumThreads];
			for (int i = 0; i < thread_done.length; i++)
				thread_done[i] = new AtomicReference<Object>();
		}

		@SuppressWarnings("unchecked")
		public AtomicReference<Object> threadDone(int id) {
			return (AtomicReference<Object>) thread_done[id];
		}
	}

	static class MTThread {
		public MTState state;
		int id;
	}

	static class MTThreadBody implements Runnable {
		MTThread t;

		public MTThreadBody(MTThread t) {
			this.t = t;
		}

		@Override
		public void run() {
			try {
				int id = t.id;
				DB db = t.state.test.db;
				long counter = 0;
				System.err.printf("... starting thread %d\n", id);
				Random0 rnd = new Random0(1000 + id);
				ByteBuf value = ByteBufFactory.defaultByteBuf();

				Pattern pattern = Pattern.compile("(\\d+)\\.(\\d+)\\.(\\d+).*");

				while (t.state.stop.get() == null) {
					t.state.counter[id].set(counter);

					long key = rnd.uniform(kNumKeys);
					String key1 = String.format("%016d", key);

					if (rnd.oneIn(2)) {
						// Write values of the form <key, my id, counter>.
						// We add some padding for force compactions.
						String value1 = String.format("%d.%d.%-1000d", key, id, counter);
						assertTrue(db.put(new WriteOptions(), new DefaultSlice(key1), new DefaultSlice(value1)).ok());
					} else {
						// Read a value and verify that it matches the pattern written above.
						Status s = db.get(new ReadOptions(), new DefaultSlice(key1), value);
						if (s.isNotFound()) {
							// Key has not yet been written
						} else {
							// Check that the writer thread counter is >= the counter in the value
							assertTrue(s.ok());
							int k = 0;
							int w = 0;
							int c = 0;
							// ASSERT_EQ(3, sscanf(value.c_str(), "%d.%d.%d", &k, &w, &c)) << value;

							Matcher m = pattern.matcher(value.encodeToString());
							assertTrue(m.find());
							// if (m.find()) {
							k = Integer.parseInt(m.group(1));
							w = Integer.parseInt(m.group(2));
							c = Integer.parseInt(m.group(3));
							// }

							assertEquals(k, key);
							assertTrue(w >= 0);
							assertTrue(w < kNumThreads);
							assertTrue(c <= t.state.counter[w].get());
						}
					}
					counter++;
				}
				t.state.threadDone(id).set(t);
				System.err.printf("... stopping thread %d after %d ops\n", id, counter);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	@Test
	public void testMultiThreaded() throws Exception {
		DBTestRunner r = new DBTestRunner();
		try {
			do {
				// Initialize state
				MTState mt = new MTState();
				mt.test = r;
				mt.stop.set(null);
				for (int id = 0; id < kNumThreads; id++) {
					mt.counter[id].set(0);
					mt.threadDone(id).set(null);
				}

				// Start threads
				MTThread[] thread = new MTThread[kNumThreads];
				for (int id = 0; id < kNumThreads; id++) {
					thread[id] = new MTThread();
					thread[id].state = mt;
					thread[id].id = id;
					r.env.startThread(new MTThreadBody(thread[id]));
				}

				// Let them run for a while
				delayMilliseconds(kTestSeconds * 1000);

				// Stop the threads and wait for them to finish
				mt.stop.set(mt);
				for (int id = 0; id < kNumThreads; id++) {
					while (mt.threadDone(id).get() == null) {
						delayMilliseconds(100);
					}
				}
			} while (r.changeOptions());
		} finally {
			r.delete();
		}
	}

	static ByteBuf randomKey(Random0 rnd) {
		int len = (int) (rnd.oneIn(3) ? 1 // Short sometimes to encourage
											// collisions
				: (rnd.oneIn(100) ? rnd.skewed(10) : rnd.uniform(10)));
		return TestUtil.randomKey(rnd, len);
	}

	static String makeKey(long num) {
		return String.format("%016d", num);
	}

	void BM_LogAndApply(int iters, int num_base_files) throws Exception {
		String dbname = TestUtil.tmpDir() + "/leveldb_test_benchmark";
		LevelDB.destroyDB(dbname, new Options());

		Object0<DB> db0 = new Object0<>();
		Options opts = new Options();
		opts.createIfMissing = true;
		Status s = LevelDB.newDB(opts, dbname, db0);
		DB db = db0.getValue();
		assertTrue(s.ok());
		assertTrue(db != null);

		db.close();
		db = null;

		Env env = LevelDB.defaultEnv();

		Mutex mutex = new Mutex();
		mutex.lock();
		try {
			InternalKeyComparator cmp = new InternalKeyComparator(BytewiseComparatorImpl.getInstance());
			Options options = new Options();
			VersionSet vset = new VersionSet(dbname, options, null, cmp);
			Boolean0 save_manifest = new Boolean0();
			assertTrue(vset.recover(save_manifest).ok());
			VersionEdit vbase = new VersionEdit();
			long fnum = 1;
			for (int i = 0; i < num_base_files; i++) {
				InternalKey start = new InternalKey(new DefaultSlice(makeKey(2 * fnum)), (long) 1, ValueType.Value);
				InternalKey limit = new InternalKey(new DefaultSlice(makeKey(2 * fnum + 1)), (long) 1, ValueType.Deletion);
				vbase.addFile(2, fnum++, 1 /* file size */, start, limit, 10);
			}
			assertTrue(vset.logAndApply(vbase, mutex).ok());

			long start_millis = env.nowMillis();

			for (int i = 0; i < iters; i++) {
				VersionEdit vedit = new VersionEdit();
				vedit.deleteFile(2, fnum);
				InternalKey start = new InternalKey(new DefaultSlice(makeKey(2 * fnum)), 1, ValueType.Value);
				InternalKey limit = new InternalKey(new DefaultSlice(makeKey(2 * fnum + 1)), 1, ValueType.Deletion);
				vedit.addFile(2, fnum++, 1 /* file size */, start, limit, 10);
				vset.logAndApply(vedit, mutex);
			}

			long stop_millis = env.nowMillis();
			long ms = stop_millis - start_millis;
			System.err.printf("BM_LogAndApply/%d   %8d iters : %d ms (%7.0f ms / iter)\n", num_base_files, iters, ms, ((float) ms) / (float) iters);
		} finally {
			mutex.unlock();
		}
	}

	@Test
	public void benchmark() throws Exception {
		BM_LogAndApply(1000, 1);
		BM_LogAndApply(1000, 100);
		BM_LogAndApply(1000, 10000);
		BM_LogAndApply(100, 100000);
	}

	static class SliceComparator implements Comparator<Slice> {
		Comparator0 cmp;

		public SliceComparator() {
			cmp = BytewiseComparatorImpl.getInstance();
		}

		public SliceComparator(Comparator0 c) {
			cmp = c;
		}

		public int compare(Slice a, Slice b) {
			return cmp.compare(a, b);
		}
	};

	static SliceComparator kSliceComparator = new SliceComparator();

	static class SliceEntry {
		public Slice key;
		public Slice value;

		public SliceEntry(Slice key, Slice value) {
			this.key = key;
			this.value = value;
		}
	}

	static class SliceEntryComparator implements Comparator<SliceEntry> {
		Comparator0 cmp;

		public SliceEntryComparator() {
			cmp = BytewiseComparatorImpl.getInstance();
		}

		public SliceEntryComparator(Comparator0 c) {
			cmp = c;
		}

		public int compare(SliceEntry a, SliceEntry b) {
			return cmp.compare(a.key, b.key);
		}

		public void setComparator(Comparator0 cmp) {
			this.cmp = cmp;
		}
	};

	static SliceEntryComparator kSliceEntryComparator = new SliceEntryComparator();

	static class ModelIter extends Iterator0 {
		ArrayList<SliceEntry> kvList = new ArrayList<>();
		boolean owned; // Do we own map
		int iterPos = -1;

		public ModelIter(TreeMap<Slice, Slice> kvmap, boolean owned) {
			for (Map.Entry<Slice, Slice> e : kvmap.entrySet()) {
				kvList.add(new SliceEntry(e.getKey(), e.getValue()));
			}
		}

		@Override
		public boolean valid() {
			return (iterPos >= 0 && iterPos < kvList.size());
		}

		@Override
		public void seekToFirst() {
			iterPos = 0;
		}

		@Override
		public void seekToLast() {
			iterPos = kvList.size() - 1;
		}

		@Override
		public void seek(Slice target) {
			SliceEntry e = new SliceEntry(target, null);
			iterPos = ListUtils.lowerBound(kvList, e, kSliceEntryComparator);
		}

		@Override
		public void next() {
			iterPos++;
		}

		@Override
		public void prev() {
			iterPos--;
		}

		@Override
		public Slice key() {
			return kvList.get(iterPos).key;
		}

		@Override
		public Slice value() {
			return kvList.get(iterPos).value;
		}

		@Override
		public Status status() {
			return Status.ok0();
		}
	}

	static class ModelDB implements DB {
		Options options;
		TreeMap<Slice, Slice> map = new TreeMap<>(kSliceComparator);

		static class ModelSnapshot extends Snapshot {
			public TreeMap<Slice, Slice> map = new TreeMap<>(kSliceComparator);
		}

		public ModelDB(Options opt) {
			options = opt.cloneOptions();
		}

		@Override
		public Status open(Options options, String name) throws Exception {
			this.options = options.cloneOptions();
			return Status.ok0();
		}

		@Override
		public Status put(WriteOptions opt, Slice key, Slice value) throws Exception {
			WriteBatch batch = new WriteBatch();
			batch.put(key, value);
			return write(opt, batch);
		}

		@Override
		public Status delete(WriteOptions opt, Slice key) throws Exception {
			WriteBatch batch = new WriteBatch();
			batch.delete(key);
			return write(opt, batch);
		}

		static class Handler implements WriteBatch.Handler {
			public TreeMap<Slice, Slice> map;

			public void put(Slice key, Slice value) {
				map.put(key, value);
			}

			public void delete(Slice key) {
				map.remove(key);
			}
		}

		@Override
		public Status write(WriteOptions options, WriteBatch updates) throws Exception {
			Handler handler = new Handler();
			handler.map = map;
			return updates.iterate(handler);
		}

		@Override
		public Status get(ReadOptions options, Slice key, ByteBuf value) throws Exception {
			assert (false);
			return Status.notFound();
		}

		@Override
		public Iterator0 newIterator(ReadOptions options) {
			if (options.snapshot == null) {
				TreeMap<Slice, Slice> saved = new TreeMap<>(kSliceComparator);
				saved.putAll(map);
				return new ModelIter(saved, true);
			} else {
				TreeMap<Slice, Slice> snapshotState = ((ModelSnapshot) (options.snapshot)).map;
				return new ModelIter(snapshotState, false);
			}
		}

		@Override
		public Snapshot getSnapshot() {
			ModelSnapshot snapshot = new ModelSnapshot();
			snapshot.map = map;
			return snapshot;
		}

		@Override
		public void releaseSnapshot(Snapshot snapshot) {

		}

		@Override
		public boolean getProperty(String property, Object0<String> value) {
			return false;
		}

		@Override
		public void getApproximateSizes(List<Range> range, List<Long> sizes) {
			for (int i = 0; i < range.size(); i++)
				sizes.add(0L);
		}

		@Override
		public void compactRange(Slice begin, Slice end) throws Exception {

		}

		@Override
		public void close() {

		}

		@Override
		public String debugDataRange() {
			return "";
		}
	}

	static boolean compareIterators(int step, DB model, DB db, Snapshot modelSnap, Snapshot dbSnap) {
		ReadOptions options = new ReadOptions();
		options.snapshot = modelSnap;
		Iterator0 miter = model.newIterator(options);
		options.snapshot = dbSnap;
		Iterator0 dbiter = db.newIterator(options);
		boolean ok = true;
		int count = 0;

		miter.seekToFirst();
		dbiter.seekToFirst();
		System.err.printf("miter.key=%s, dbiter.key=%s\n", Strings.escapeString(miter.key()), Strings.escapeString(dbiter.key()));

		for (miter.seekToFirst(), dbiter.seekToFirst(); ok && miter.valid() && dbiter.valid(); miter.next(), dbiter.next()) {
			count++;
			if (miter.key().compare(dbiter.key()) != 0) {
				System.err.printf("step %d: Key mismatch: '%s' vs. '%s'\n", step, Strings.escapeString(miter.key()), Strings.escapeString(dbiter.key()));
				ok = false;
				break;
			}

			if (miter.value().compare(dbiter.value()) != 0) {
				System.err.printf("step %d: Value mismatch for key '%s': '%s' vs. '%s'\n", step, Strings.escapeString(miter.key()), Strings.escapeString(miter.value()),
						Strings.escapeString(miter.value()));
				ok = false;
			}
		}

		if (ok) {
			if (miter.valid() != dbiter.valid()) {
				System.err.printf("step %d: Mismatch at end of iterators: %d vs. %d\n", step, miter.valid(), dbiter.valid());
				ok = false;
			}
		}

		System.err.printf("%d entries compared: ok=%s\n", count, ok);
		miter.delete();
		dbiter.delete();

		return ok;
	}

	//TODO
	public void testRandomized() throws Exception {
		Random0 rnd = new Random0(TestUtil.randomSeed());

		DBTestRunner r = new DBTestRunner();
		try {
			do {
				ModelDB model = new ModelDB(r.currentOptions().cloneOptions());
				final int N = 10000;
				Snapshot modelSnap = null;
				Snapshot dbSnap = null;
				ByteBuf k = null;
				ByteBuf v = null;

				for (int step = 0; step < N; step++) {
					if (step % 100 == 0) {
						System.err.printf("Step %d of %d\n", step, N);
					}
					// TODO(sanjay): Test Get() works
					long p = rnd.uniform(100);
					if (p < 45) { // Put
						k = randomKey(rnd);
						v = randomString(rnd, (int) (rnd.oneIn(20) ? 100 + rnd.uniform(100) : rnd.uniform(8)));
						assertTrue(model.put(new WriteOptions(), k, v).ok());
						assertTrue(r.db.put(new WriteOptions(), k, v).ok());

					} else if (p < 90) { // Delete
						k = randomKey(rnd);
						assertTrue(model.delete(new WriteOptions(), k).ok());
						assertTrue(r.db.delete(new WriteOptions(), k).ok());

					} else { // Multi-element batch
						WriteBatch b = new WriteBatch();
						final long num = rnd.uniform(8);
						for (int i = 0; i < num; i++) {
							if (i == 0 || !rnd.oneIn(10)) {
								k = randomKey(rnd);
							} else {
								// Periodically re-use the same key from the previous iter, so
								// we have multiple entries in the write batch for the same key
							}
							if (rnd.oneIn(2)) {
								v = randomString(rnd, (int) rnd.uniform(10));
								b.put(k, v);
							} else {
								b.delete(k);
							}
						}
						assertTrue(model.write(new WriteOptions(), b).ok());
						assertTrue(r.db.write(new WriteOptions(), b).ok());
					}

					if ((step % 100) == 0) {
						assertTrue(compareIterators(step, model, r.db, null, null));
						assertTrue(compareIterators(step, model, r.db, modelSnap, dbSnap));
						// Save a snapshot from each DB this time that we'll use next
						// time we compare things, to make sure the current state is
						// preserved with the snapshot
						if (modelSnap != null)
							model.releaseSnapshot(modelSnap);
						if (dbSnap != null)
							r.db.releaseSnapshot(dbSnap);

						r.reopen();
						assertTrue(compareIterators(step, model, r.db, null, null));

						modelSnap = model.getSnapshot();
						dbSnap = r.db.getSnapshot();
					}
				}
				if (modelSnap != null)
					model.releaseSnapshot(modelSnap);
				if (dbSnap != null)
					r.db.releaseSnapshot(dbSnap);
			} while (r.changeOptions());

		} finally {
			r.delete();
		}
	}
}
