package com.tchaicatkovsky.jleveldb.test;

import org.junit.Test;

import com.tchaicatkovsky.jleveldb.DB;
import com.tchaicatkovsky.jleveldb.Env;
import com.tchaicatkovsky.jleveldb.FileName;
import com.tchaicatkovsky.jleveldb.FileType;
import com.tchaicatkovsky.jleveldb.LevelDB;
import com.tchaicatkovsky.jleveldb.Logger0;
import com.tchaicatkovsky.jleveldb.Options;
import com.tchaicatkovsky.jleveldb.ReadOptions;
import com.tchaicatkovsky.jleveldb.Snapshot;
import com.tchaicatkovsky.jleveldb.Status;
import com.tchaicatkovsky.jleveldb.WritableFile;
import com.tchaicatkovsky.jleveldb.WriteBatch;
import com.tchaicatkovsky.jleveldb.WriteOptions;
import com.tchaicatkovsky.jleveldb.db.DBImpl;
import com.tchaicatkovsky.jleveldb.db.LogWriter;
import com.tchaicatkovsky.jleveldb.db.WriteBatchInternal;
import com.tchaicatkovsky.jleveldb.util.ByteBuf;
import com.tchaicatkovsky.jleveldb.util.ByteBufFactory;
import com.tchaicatkovsky.jleveldb.util.Long0;
import com.tchaicatkovsky.jleveldb.util.Object0;
import com.tchaicatkovsky.jleveldb.util.Slice;
import com.tchaicatkovsky.jleveldb.util.SliceFactory;
import com.tchaicatkovsky.jleveldb.util.Utils;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;

public class TestRecovery {
	static {
		Logger0.disableLogger0();
	}
	
	static class RecoveryRunner {
		String dbname = Utils.tmpDir() + "/recovery_test";
		Env env;
		DB db;
		
		public RecoveryRunner() throws Exception {
			env = LevelDB.defaultEnv();
			assertNotNull(env);
			db = null;
			LevelDB.destroyDB(dbname, new Options());
			open();
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
			if (db != null) {
			    db.close();
			    db = null;
			}
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
		    return db.put(WriteOptions.defaultOne(), SliceFactory.newUnpooled(k), SliceFactory.newUnpooled(v));
		}
		
		public Status put(ByteBuf k, ByteBuf v) throws Exception {
		    return db.put(WriteOptions.defaultOne(), SliceFactory.newUnpooled(k), SliceFactory.newUnpooled(v));
		}
		
		public ByteBuf get(Slice k, Snapshot snapshot) throws Exception {
		    ByteBuf result = ByteBufFactory.newUnpooled();
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
			return get(SliceFactory.newUnpooled(s), null);
		}
		
		String manifestFileName() {
			ByteBuf current = ByteBufFactory.newUnpooled();
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
		
		public String tblName(long number) {
			return FileName.getTableFileName(dbname, number);
		}
		
		public int deleteLogFiles() {
		    ArrayList<Long> logs = getFiles(FileType.LogFile);
		    
		    for (int i = 0; i < logs.size(); i++) {
		    	String logName = logName(logs.get(i));
		    	Status s = env.deleteFile(logName);
		    	assertTrue(s.ok());
		    }
	
		    return logs.size();
		}
		
		public int deleteTableFiles() {
			ArrayList<Long> tbls = getFiles(FileType.TableFile);

		    for (int i = 0; i < tbls.size(); i++) {
		    	String tblName = tblName(tbls.get(i));
		    	Status s = env.deleteFile(tblName);
		    	assertTrue(s.ok());
		    }
		    return tbls.size();
		}
		
		public void dumpTableFiles() {
			System.out.println("\n[DEBUG] dump table files: ");
			ArrayList<Long> tbls = getFiles(FileType.TableFile);
			for (int i = 0; i < tbls.size(); i++) {
		    	String tblName = tblName(tbls.get(i));
		    	System.out.printf("[DEBUG] tbl name: %s\n", tblName);
		    }
		}
		
		public void dumpLogFiles() {
			System.out.println("\n[DEBUG] dump log files: ");
			ArrayList<Long> tbls = getFiles(FileType.LogFile);
			for (int i = 0; i < tbls.size(); i++) {
		    	String logName = logName(tbls.get(i));
		    	System.out.printf("[DEBUG] log name: %s\n", logName);
		    }
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
		
		r.delete();
	}
	
	//TODO
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
			ByteBuf zeroes = ByteBufFactory.newUnpooled();
			    
			for (int i = 0; i < (long)(3*1048576) - len; i++) {
				zeroes.addByte((byte)0);
			}
			    
			assertTrue(file0.getValue().append(SliceFactory.newUnpooled(zeroes)).ok());
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
		
		r.delete();
	}
	
	@Test
	public void testNoLogFiles() throws Exception {
		RecoveryRunner r = new RecoveryRunner();
		
		assertTrue(r.put("foo", "bar").ok());
		
		r.close();
		
		assertEquals(1, r.deleteLogFiles());
		
		r.open();
		
		assertEquals("NOT_FOUND", r.get("foo").encodeToString());
		r.open();
		assertEquals("NOT_FOUND", r.get("foo").encodeToString());
		
		r.delete();
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
		
		r.delete();
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
		long oldLogFile = r.firstLogFile();

		// Force creation of multiple memtables by reducing the write buffer size.
		Options opt = new Options();
		opt.reuseLogs = true;
		opt.writeBufferSize = (kNum*100) / 2;
		r.open(opt);
		assertTrue(2 <= r.numTables());
		assertEquals(1, r.numLogs());
		assertTrue(oldLogFile != r.firstLogFile());
		for (int i = 0; i < kNum; i++) {
			String s = String.format("%050d", i);
			assertEquals(s, r.get(s).encodeToString());
		}
		
		r.delete();
	}
	
	@Test
	public void testMultipleLogFiles() throws Exception {
		RecoveryRunner r = new RecoveryRunner();
		assertTrue(r.put("foo", "bar").ok());
		r.close();
		
		assertEquals(1, r.numLogs());
		
		// Make a bunch of uncompacted log files.
		long oldLog = r.firstLogFile();
		r.makeLogFile(oldLog+1, 1000, SliceFactory.newUnpooled("hello"), SliceFactory.newUnpooled("world"));
		r.makeLogFile(oldLog+2, 1001, SliceFactory.newUnpooled("hi"), SliceFactory.newUnpooled("there"));
		r.makeLogFile(oldLog+3, 1002, SliceFactory.newUnpooled("foo"), SliceFactory.newUnpooled("bar2"));

		
		// Recover and check that all log files were processed.
		r.open();
		
		assertTrue(1 <= r.numTables());
		assertEquals(1, r.numLogs());
		long newLog = r.firstLogFile();
		
		assertTrue(oldLog+3 <= newLog);
		assertEquals("bar2", r.get("foo").encodeToString());
		assertEquals("world", r.get("hello").encodeToString());
		assertEquals("there", r.get("hi").encodeToString());

		// Test that previous recovery produced recoverable state.
		r.open();
		assertTrue(1 <= r.numTables());
		assertEquals(1, r.numLogs());
		if (r.canAppend()) {
			assertEquals(newLog, r.firstLogFile());
		}
		assertEquals("bar2", r.get("foo").encodeToString());
		assertEquals("world", r.get("hello").encodeToString());
		assertEquals("there", r.get("hi").encodeToString());

		// Check that introducing an older log file does not cause it to be re-read.
		r.close();
		r.makeLogFile(oldLog+1, 2000, SliceFactory.newUnpooled("hello"), SliceFactory.newUnpooled("stale write"));
		r.open();
		assertTrue(1 <= r.numTables());
		assertEquals(1, r.numLogs());
		if (r.canAppend()) {
			assertEquals(newLog, r.firstLogFile());
		}
		assertEquals("bar2", r.get("foo").encodeToString());
		assertEquals("world", r.get("hello").encodeToString());
		assertEquals("there", r.get("hi").encodeToString());
		
		r.delete();
	}
}
