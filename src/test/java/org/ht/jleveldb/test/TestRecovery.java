package org.ht.jleveldb.test;

import org.ht.jleveldb.DB;
import org.ht.jleveldb.Env;
import org.ht.jleveldb.FileName;
import org.ht.jleveldb.FileType;
import org.ht.jleveldb.LevelDB;
import org.ht.jleveldb.Options;
import org.ht.jleveldb.ReadOptions;
import org.ht.jleveldb.Snapshot;
import org.ht.jleveldb.Status;
import org.ht.jleveldb.WritableFile;
import org.ht.jleveldb.WriteBatch;
import org.ht.jleveldb.WriteOptions;
import org.ht.jleveldb.db.DBImpl;
import org.ht.jleveldb.db.LogWriter;
import org.ht.jleveldb.db.WriteBatchInternal;
import org.ht.jleveldb.util.ByteBuf;
import org.ht.jleveldb.util.ByteBufFactory;
import org.ht.jleveldb.util.Long0;
import org.ht.jleveldb.util.Object0;
import org.ht.jleveldb.util.Slice;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;

public class TestRecovery {
	static class RecoveryRunner {
		String dbname;
		Env env;
		DB db;
		
		public RecoveryRunner() {
			env = LevelDB.defaultEnv();
			assertNotNull(env);
			db = null;
		}
		
		public void delete() {
			close();
			try {
				LevelDB.destroyDB(dbname, new Options());
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		public DBImpl dbfull() {  
			return (DBImpl)db; 
		}
		
		public Env env() { 
			return env; 
		}
		
		public boolean canAppend() {
		    Object0<WritableFile> tmp0 = new Object0<>();
		    Status s = env.newAppendableFile(FileName.getCurrentFileName(dbname), tmp0);
		    if (tmp0.getValue() != null)
		    	tmp0.getValue().delete();
		    
		    if (s.isOtherError()) {
		    	return false;
		    } else {
		    	return true;
		    }
		}
		
		public void close() {
		    db.close();
		    db = null;
		}
		
		public void open(Options options) {
		    close();
		    Options opts = new Options();
		    if (options != null) {
		    	opts = options.cloneOptions();
		    } else {
		    	opts.reuseLogs = true;  // TODO(sanjay): test both ways
		    	opts.createIfMissing = true;
		    }
		    if (opts.env == null) {
		    	opts.env = env;
		    }
		    Object0<DB> db0 = new Object0<>();
		    try {
		    	Status s = LevelDB.newDB(opts, dbname, db0);
		    	assertTrue(s.ok());
		    	db = db0.getValue();
		    } catch (Exception e) {
		    	e.printStackTrace();
		    	assertTrue(false);
		    }
		    assertEquals(1, numLogs());
		}

		public void open() {
			open(null);
		}
		
		public Status put(String k, String v) throws Exception {
		    return db.put(WriteOptions.defaultOne(), new Slice(k), new Slice(v));
		}
		
		public Status put(ByteBuf k, ByteBuf v) throws Exception {
		    return db.put(WriteOptions.defaultOne(), new Slice(k), new Slice(v));
		}
		
		public ByteBuf get(Slice k, Snapshot snapshot) throws Exception {
		    ByteBuf result = ByteBufFactory.defaultByteBuf();
		    Status s = db.get(new ReadOptions(), k, result);
		    if (s.isNotFound()) {
		    	result.assign("NOT_FOUND");
		    } else if (!s.ok()) {
		    	result.assign(""+s);
		    }
		    return result;
		}
		
		public ByteBuf get(Slice k) throws Exception {
			return get(k, null);
		}
		
		public ByteBuf get(String s) throws Exception {
			return get(new Slice(s), null);
		}
		
		String manifestFileName() {
			ByteBuf current = ByteBufFactory.defaultByteBuf();
			Status s = env.readFileToString(FileName.getCurrentFileName(dbname), current);
			assertTrue(s.ok());
			int len = current.size();
			if (len > 0 && current.getByte(len-1) == ('\n' - '\0')) {
				current.resize(len - 1);
			}
			return dbname + "/" + current.encodeToString();
		}
		
		public String logName(long number) {
		    return FileName.getLogFileName(dbname, number);
		}
		
		public int deleteLogFiles() {
		    ArrayList<Long> logs = getFiles(FileType.LogFile);
		    for (int i = 0; i < logs.size(); i++) {
		    	Status s = env.deleteFile(logName(logs.get(i)));
		    	assertTrue(s.ok());
		    }
		    return logs.size();
		}
		
		public long firstLogFile() {
		    return getFiles(FileType.LogFile).get(0);
		}
		
		public ArrayList<Long> getFiles(FileType t) {
			ArrayList<String> filenames = new ArrayList<>();
		    Status s = env.getChildren(dbname, filenames);
		    assertTrue(s.ok());
		    
		    ArrayList<Long> result = new ArrayList<>();
		    for (int i = 0; i < filenames.size(); i++) {
		    	Long0 number = new Long0();
		    	Object0<FileType> type = new Object0<>();
		    	if (FileName.parseFileName(filenames.get(i), number, type) && type.getValue() == t) {
		    		result.add(number.getValue());
		    	}
		    }
		    return result;
		}
		
		public int numLogs() {
		    return getFiles(FileType.LogFile).size();
		}
		
		public int numTables() {
		    return getFiles(FileType.TableFile).size();
		}
		
		public long fileSize(String fname) {
		    Long0 result = new Long0();
		    Status s = env.getFileSize(fname, result);
		    assertTrue(s.ok());
		    return result.getValue();
		}
		
		public void compactMemTable() throws Exception {
		    dbfull().TEST_CompactMemTable();
		}
		
		// Directly construct a log file that sets key to val.
		public void makeLogFile(long lognum, long seq, Slice key, Slice val) {
		    String fname = FileName.getLogFileName(dbname, lognum);
		    Object0<WritableFile> file0 = new Object0<>();
		    Status s = env.newWritableFile(fname, file0);
		    assertTrue(s.ok());
		    LogWriter writer = new LogWriter(file0.getValue());
		    WriteBatch batch = new WriteBatch();
		    batch.put(key, val);
		    WriteBatchInternal.setSequence(batch, seq);
		    assertTrue(writer.addRecord(WriteBatchInternal.contents(batch)).ok());
		    assertTrue(file0.getValue().flush().ok());
		    file0.getValue().delete();;
		}
	}
	
	@Test
	public void testManifestReused() throws Exception {
		RecoveryRunner r = new RecoveryRunner();
		if (!r.canAppend()) {
		    System.err.printf("skipping test because env does not support appending\n");
		    return;
		}
		assertTrue(r.put("foo", "bar").ok());
		r.close();
		String old_manifest = r.manifestFileName();
		r.open();
		assertEquals(old_manifest, r.manifestFileName());
		assertEquals("bar", r.get("foo").encodeToString());
		r.open();
		assertEquals(old_manifest, r.manifestFileName());
		assertEquals("bar", r.get("foo").encodeToString());
	}
	
	
	@Test
	public void testLargeManifestCompacted() throws Exception {
		RecoveryRunner r = new RecoveryRunner();
		if (!r.canAppend()) {
			System.err.printf("skipping test because env does not support appending\n");
			return;
		}
		assertTrue(r.put("foo", "bar").ok());
		r.close();
		String old_manifest = r.manifestFileName();

		// Pad with zeroes to make manifest file very big.
		{
			long len = r.fileSize(old_manifest);
			Object0<WritableFile> file0 = new Object0<>();
			assertTrue(r.env.newAppendableFile(old_manifest, file0).ok());
			ByteBuf zeroes = ByteBufFactory.defaultByteBuf();
			    
			for (int i = 0; i < (long)(3*1048576) - len; i++) {
				zeroes.addByte((byte)0);
			}
			    
			assertTrue(file0.getValue().append(new Slice(zeroes)).ok());
			assertTrue(file0.getValue().flush().ok());
			file0.getValue().delete();
		}

		r.open();
		String new_manifest = r.manifestFileName();
		assertEquals(old_manifest, new_manifest);
		assertTrue(10000 > r.fileSize(new_manifest));
		assertEquals("bar", r.get("foo").encodeToString());

		r.open();
		assertEquals(new_manifest, r.manifestFileName());
		assertEquals("bar", r.get("foo").encodeToString());
	}
	
	@Test
	public void testNoLogFiles() throws Exception {
		RecoveryRunner r = new RecoveryRunner();
		assertTrue(r.put("foo", "bar").ok());
		assertEquals(1, r.deleteLogFiles());
		r.open();
		assertEquals("NOT_FOUND", r.get("foo").encodeToString());
		r.open();
		assertEquals("NOT_FOUND", r.get("foo").encodeToString());
	}
	
	@Test
	public void testLogFileReuse() throws Exception {
		RecoveryRunner r = new RecoveryRunner();
		if (!r.canAppend()) {
		    System.err.printf("skipping test because env does not support appending\n");
		    return;
		}
		for (int i = 0; i < 2; i++) {
			assertTrue(r.put("foo", "bar").ok());
			if (i == 0) {
				// Compact to ensure current log is empty
				r.compactMemTable();
			}
			r.close();
			assertEquals(1, r.numLogs());
			long number = r.firstLogFile();
			if (i == 0) {
				assertEquals(0, r.fileSize(r.logName(number)));
			} else {
				assertTrue(0 < r.fileSize(r.logName(number)));
			}
			r.open();
			assertEquals(1, r.numLogs());
			assertEquals(number, r.firstLogFile());
			assertEquals("bar", r.get("foo").encodeToString());
			r.open();
			assertEquals(1, r.numLogs());
			assertEquals(number, r.firstLogFile());
			assertEquals("bar", r.get("foo").encodeToString());
		}
	}
	
	@Test
	public void testMultipleMemTables() throws Exception {
		RecoveryRunner r = new RecoveryRunner();
		// Make a large log.
		int kNum = 1000;
		for (int i = 0; i < kNum; i++) {
			String s = String.format("%050d", i);
			assertTrue(r.put(s, s).ok());
		}
		assertEquals(0, r.numTables());
		r.close();
		assertEquals(0, r.numTables());
		assertEquals(1, r.numLogs());
		long old_log_file = r.firstLogFile();

		// Force creation of multiple memtables by reducing the write buffer size.
		Options opt = new Options();
		opt.reuseLogs = true;
		opt.writeBufferSize = (kNum*100) / 2;
		r.open(opt);
		assertTrue(2 < r.numTables());
		assertEquals(1, r.numLogs());
		assertTrue(old_log_file != r.firstLogFile());
		for (int i = 0; i < kNum; i++) {
			String s = String.format("%050d", i);
			assertEquals(s, r.get(s));
		}
	}
	
	@Test
	public void testMultipleLogFiles() throws Exception {
		RecoveryRunner r = new RecoveryRunner();
		assertTrue(r.put("foo", "bar").ok());
		r.close();
		assertEquals(1, r.numLogs());

		// Make a bunch of uncompacted log files.
		long old_log = r.firstLogFile();
		r.makeLogFile(old_log+1, 1000, new Slice("hello"), new Slice("world"));
		r.makeLogFile(old_log+2, 1001, new Slice("hi"), new Slice("there"));
		r.makeLogFile(old_log+3, 1002, new Slice("foo"), new Slice("bar2"));

		// Recover and check that all log files were processed.
		r.open();
		assertTrue(1 < r.numTables());
		assertEquals(1, r.numLogs());
		long new_log = r.firstLogFile();
		assertTrue(old_log+3 < new_log);
		assertEquals("bar2", r.get("foo").encodeToString());
		assertEquals("world", r.get("hello").encodeToString());
		assertEquals("there", r.get("hi").encodeToString());

		// Test that previous recovery produced recoverable state.
		r.open();
		assertTrue(1 < r.numTables());
		assertEquals(1, r.numLogs());
		if (r.canAppend()) {
			assertEquals(new_log, r.firstLogFile());
		}
		assertEquals("bar2", r.get("foo").encodeToString());
		assertEquals("world", r.get("hello").encodeToString());
		assertEquals("there", r.get("hi").encodeToString());

		// Check that introducing an older log file does not cause it to be re-read.
		r.close();
		r.makeLogFile(old_log+1, 2000, new Slice("hello"), new Slice("stale write"));
		r.open();
		assertTrue(1 < r.numTables());
		assertEquals(1, r.numLogs());
		if (r.canAppend()) {
			assertTrue(new_log < r.firstLogFile());
		}
		assertEquals("bar2", r.get("foo").encodeToString());
		assertEquals("world", r.get("hello").encodeToString());
		assertEquals("there", r.get("hi").encodeToString());
	}
}
