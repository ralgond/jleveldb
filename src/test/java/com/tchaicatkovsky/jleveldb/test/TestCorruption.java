/**
 * Copyright (c) 2017-2018, Teng Huang <ht201509 at 163 dot com>
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
 */

package com.tchaicatkovsky.jleveldb.test;

import com.tchaicatkovsky.jleveldb.DB;
import com.tchaicatkovsky.jleveldb.FileName;
import com.tchaicatkovsky.jleveldb.FileType;
import com.tchaicatkovsky.jleveldb.Iterator0;
import com.tchaicatkovsky.jleveldb.LevelDB;
import com.tchaicatkovsky.jleveldb.Logger0;
import com.tchaicatkovsky.jleveldb.Options;
import com.tchaicatkovsky.jleveldb.ReadOptions;
import com.tchaicatkovsky.jleveldb.Status;
import com.tchaicatkovsky.jleveldb.WriteBatch;
import com.tchaicatkovsky.jleveldb.WriteOptions;
import com.tchaicatkovsky.jleveldb.db.DBImpl;
import com.tchaicatkovsky.jleveldb.db.LogFormat;
import com.tchaicatkovsky.jleveldb.db.format.DBFormat;
import com.tchaicatkovsky.jleveldb.util.ByteBuf;
import com.tchaicatkovsky.jleveldb.util.ByteBufFactory;
import com.tchaicatkovsky.jleveldb.util.Cache;
import com.tchaicatkovsky.jleveldb.util.Long0;
import com.tchaicatkovsky.jleveldb.util.Object0;
import com.tchaicatkovsky.jleveldb.util.Random0;
import com.tchaicatkovsky.jleveldb.util.Slice;
import com.tchaicatkovsky.jleveldb.util.SliceFactory;
import com.tchaicatkovsky.jleveldb.util.Utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;

import org.junit.Test;

public class TestCorruption {
	
	static {
		Logger0.disableLogger0();
	}
	
	private String getMethodName() {  
        StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();  
        StackTraceElement e = stacktrace[2];  
        String methodName = e.getMethodName();  
        return methodName;  
    }  
	
	static final int kValueSize = 1000;
	
	static class CorruptionRunner {
		ErrorEnv env = new ErrorEnv();
		String dbname;
		Cache tinyCache;
		Options options = new Options();
		DB db;
		
		public CorruptionRunner() throws Exception {
			tinyCache = Cache.newLRUCache(100);
		    options.env = env;
		    options.blockCache = tinyCache;
		    dbname = Utils.tmpDir() + "/corruption_test";
		    LevelDB.destroyDB(dbname, options);

		    db = null;
		    options.createIfMissing = true;
		    reopen();
		    options.createIfMissing = false;
		}
		
		public void close() {
			if (db != null) {
				db.close();
				db = null;
			}
		}
		
		public void delete() throws Exception {
			close();
			tinyCache.delete();
			LevelDB.destroyDB(dbname, new Options());
			tinyCache = null;
		}

		public Status tryReopen() throws Exception {
		    close();
		    Object0<DB> db0 = new Object0<>();
		    Status s = LevelDB.newDB(options, dbname, db0);
		    db = db0.getValue();
//		    if (s.ok()) {
//		    	DBImpl dbi = (DBImpl)db;
//		    	dbi.Test_setTableCacheZero();
//		    }
		    return s;
		}

		public void reopen() throws Exception {
		    assertTrue(tryReopen().ok());
		}

		public void repairDB() throws Exception {
		    close();
		    assertTrue(LevelDB.repairDB(dbname, options).ok());
		}
		
		public void build(int n) throws Exception {
		    ByteBuf keySpace = ByteBufFactory.newUnpooled();
		    ByteBuf valueSpace = ByteBufFactory.newUnpooled();
		    
		    WriteBatch batch = new WriteBatch();
		    for (int i = 0; i < n; i++) {
		    	//if ((i % 100) == 0) fprintf(stderr, "@ %d of %d\n", i, n);
		    	Slice key = Key(i, keySpace);
		    	batch.clear();
		    	batch.put(key, Value(i, valueSpace));
		    	WriteOptions options = new WriteOptions();
		    	// Corrupt() doesn't work without this sync on windows; stat reports 0 for
		    	// the file size.
		    	if (i == n - 1) {
		    		options.sync = true;
		    	}
		    	assertTrue(db.write(options, batch).ok());
		    }
		}
		
		public void check(int minExpected, int maxExpected) {
		    long nextExpected = 0;
		    int missed = 0;
		    int badKeys = 0;
		    int badValues = 0;
		    int correct = 0;
		    ByteBuf valueSpace = ByteBufFactory.newUnpooled(); 
		    
		    Iterator0 iter = db.newIterator(new ReadOptions());
		    iter.seekToFirst();
		    for (; iter.valid(); iter.next()) {
		    	String ins = iter.key().encodeToString();
		    	if (ins.equals("") || ins.equals("~")) {
		    		// Ignore boundary keys.
		    		continue;
		    	}
		    	Object0<String> ins0 = new Object0<>(ins);
		    	Long0 key0 = new Long0();
		    	if (!FileName.consumeDecimalNumber(ins0, key0) || 
		    			!ins0.getValue().isEmpty() || 
		    			key0.getValue() < nextExpected) {
		    		badKeys++;
		    		continue;
		    	}
		    	missed += (key0.getValue() - nextExpected);
		    	nextExpected = key0.getValue() + 1;
		    	if (!iter.value().equals(Value(key0.getValue(), valueSpace))) {
		    		badValues++;
		    	} else {
		    		correct++;
		    	}
		    }
		    iter.delete();
		    iter = null;

		    System.err.printf("expected=%d..%d; got=%d; bad_keys=%d; bad_values=%d; missed=%d\n",
		            minExpected, maxExpected, correct, badKeys, badValues, missed);
		    assertTrue(minExpected <= correct);
		    assertTrue(maxExpected >= correct);
		}
		
		public void corrupt(FileType filetype, long offset, long bytesToCorrupt) {
			// Pick file to corrupt
			ArrayList<String> filenames = new ArrayList<>();
			assertTrue(env.getChildren(dbname, filenames).ok());
			Long0 number0 = new Long0();
			Object0<FileType> type0 = new Object0<>();
			String fname = "";
			int pickedNumber = -1;
			
			// Pick latest file
			for (int i = 0; i < filenames.size(); i++) {
				if (FileName.parseFileName(filenames.get(i), number0, type0) &&
			          type0.getValue() == filetype &&
			          (int)number0.getValue() > pickedNumber) {  
			        fname = dbname + "/" + filenames.get(i);
			        pickedNumber = (int)number0.getValue();
				}
			}
			assertTrue(!fname.isEmpty());
			
			Long0 fileSize0 = new Long0();
			assertTrue(env.getFileSize(fname, fileSize0).ok());
			long fileSize = fileSize0.getValue();
			if (offset < 0) {
				// Relative to end of file; make it absolute
				if (-offset > fileSize)
					offset = 0;
				else
					offset = fileSize + offset;
				
			}
			if (offset > fileSize)
				offset = fileSize;
			
			if (offset + bytesToCorrupt > fileSize)
				bytesToCorrupt = fileSize - offset;

			// Do it
			ByteBuf contents = ByteBufFactory.newUnpooled();
			Status s = env.readFileToString(fname, contents);
			assertTrue(s.ok());
			byte[] data = contents.data();
			for (int i = 0; i < bytesToCorrupt; i++) {
				//contents[i + offset] ^= 0x80;
				data[(int)(i+offset)] = (byte)(((data[(int)(i+offset)] & 0xff) ^ 0x80) & 0xff);
			}
			s = env.writeStringToFile(SliceFactory.newUnpooled(contents), fname);
			assertTrue(s.ok());
		}
		
		public int property(String name) {
		    Object0<String> property = new Object0<>();
		    if (db.getProperty(name, property)) {
		        try {
		        	return Integer.parseInt(property.getValue());
		        } catch (Exception e){
		        	return -1;
		        }
		    } else {
		      return -1;
		    }
		}
	}
	
	@Test
	public void testRecovery() throws Exception {
		System.err.println("Start "+getMethodName()+":");
		CorruptionRunner r = null;
		try {
			r = new CorruptionRunner();
			r.build(100);
			r.check(100, 100);
			r.corrupt(FileType.LogFile, 19, 1); // WriteBatch tag for first record
			r.corrupt(FileType.LogFile, LogFormat.kBlockSize + 1000, 1); // Somewhere in second block
			r.reopen();

			// The 64 records in the first two log blocks are completely lost.
			r.check(36, 36);
		} catch (Exception e) {
			e.printStackTrace();
			assertTrue(false);
		} finally {
			r.delete();
		}
	}
	
	@Test
	public void testRecoverWriteError() throws Exception {
		System.err.println("Start "+getMethodName()+":");
		CorruptionRunner r = null;
		try {
			r = new CorruptionRunner();
			r.env.writableFileError = true;
			Status s = r.tryReopen();
			assertTrue(!s.ok());
		} catch (Exception e) {
			e.printStackTrace();
			assertTrue(false);
		} finally {
			r.delete();
		}
	}
	
	@Test
	public void testNewFileErrorDuringWrite() throws Exception {
		System.err.println("Start "+getMethodName()+":");
		CorruptionRunner r = null;
		try {
			// Do enough writing to force minor compaction
			r = new CorruptionRunner();
			r.env.writableFileError = true;
			int num = 3 + (new Options()).writeBufferSize / kValueSize;
			ByteBuf valueStorage = ByteBufFactory.newUnpooled();
			Status s = Status.ok0();
			for (int i = 0; s.ok() && i < num; i++) {
				WriteBatch batch = new WriteBatch();
				batch.put(SliceFactory.newUnpooled("a"), Value(100, valueStorage));
				s = r.db.write(new WriteOptions(), batch);
			}
			assertTrue(!s.ok());
			assertTrue(r.env.numWritableFileErrors >= 1);
			r.env.writableFileError = false;
			r.reopen();
		} catch (Exception e) {
			e.printStackTrace();
			assertTrue(false);
		} finally {
			r.delete();
		}
	}
	
	@Test
	public void testTableFile() throws Exception {
		System.err.println("Start "+getMethodName()+":");
		CorruptionRunner r = null;
		try {
			r = new CorruptionRunner();
			r.build(100);
			DBImpl dbi = (DBImpl) (r.db);
			dbi.TEST_CompactMemTable();
			dbi.TEST_CompactRange(0, null, null);
			dbi.TEST_CompactRange(1, null, null);

			dbi.TEST_cleanTableCache();
			
			r.corrupt(FileType.TableFile, 100, 1);
			r.check(90, 99);
		} catch (Exception e) {
			e.printStackTrace();
			assertTrue(false);
		} finally {
			r.delete();
		}
	}
	
	@Test
	public void testTableFileRepair() throws Exception {
		System.err.println("Start "+getMethodName()+":");
		CorruptionRunner r = null;
		try {
			r = new CorruptionRunner();
			r.options.blockSize = 2 * kValueSize; // Limit scope of corruption
			r.options.paranoidChecks = true;
			r.reopen();
			r.build(100);
			DBImpl dbi = (DBImpl) (r.db);
			dbi.TEST_CompactMemTable();
			dbi.TEST_CompactRange(0, null, null);
			dbi.TEST_CompactRange(1, null, null);

			dbi.TEST_cleanTableCache();
			
			r.corrupt(FileType.TableFile, 100, 1);
			r.repairDB();
			r.reopen();
			r.check(95, 99);
		} catch (Exception e) {
			e.printStackTrace();
			assertTrue(false);
		} finally {
			r.delete();
		}
	}
	
	@Test
	public void testTableFileIndexData() throws Exception {
		System.err.println("Start "+getMethodName()+":");
		CorruptionRunner r = null;
		try {
			r = new CorruptionRunner();
			r.build(10000); // Enough to build multiple Tables
			DBImpl dbi = (DBImpl) (r.db);
			dbi.TEST_CompactMemTable();

			dbi.TEST_cleanTableCache();
			
			r.corrupt(FileType.TableFile, -2000, 500);
			r.reopen();
			r.check(5000, 9999);
		} catch (Exception e) {
			e.printStackTrace();
			assertTrue(false);
		} finally {
			r.delete();
		}
	}
	
	@Test
	public void testMissingDescriptor() throws Exception {
		System.err.println("Start "+getMethodName()+":");
		CorruptionRunner r = null;
		try {
			r = new CorruptionRunner();
			r.build(1000);
			r.repairDB();
			r.reopen();
			r.check(1000, 1000);
		} catch (Exception e) {
			e.printStackTrace();
			assertTrue(false);
		} finally {
			r.delete();
		}
	}
	
	static Slice S0(String s) {
		return SliceFactory.newUnpooled(s);
	}
	
	@Test
	public void testSequenceNumberRecovery() throws Exception {
		System.err.println("Start "+getMethodName()+":");
		CorruptionRunner r = null;
		try {
			r = new CorruptionRunner();
			assertTrue(r.db.put(new WriteOptions(), S0("foo"), S0("v1")).ok());
			assertTrue(r.db.put(new WriteOptions(), S0("foo"), S0("v2")).ok());
			assertTrue(r.db.put(new WriteOptions(), S0("foo"), S0("v3")).ok());
			assertTrue(r.db.put(new WriteOptions(), S0("foo"), S0("v4")).ok());
			assertTrue(r.db.put(new WriteOptions(), S0("foo"), S0("v5")).ok());
			r.repairDB();
			r.reopen();
			ByteBuf v = ByteBufFactory.newUnpooled();
			assertTrue(r.db.get(new ReadOptions(), S0("foo"), v).ok());
			assertEquals("v5", v.encodeToString());
			// Write something. If sequence number was not recovered properly,
			// it will be hidden by an earlier write.
			assertTrue(r.db.put(new WriteOptions(), S0("foo"), S0("v6")).ok());
			assertTrue(r.db.get(new ReadOptions(), S0("foo"), v).ok());
			assertEquals("v6", v.encodeToString());
			r.reopen();
			assertTrue(r.db.get(new ReadOptions(), S0("foo"), v).ok());
			assertEquals("v6", v.encodeToString());
		} catch (Exception e) {
			e.printStackTrace();
			assertTrue(false);
		} finally {
			r.delete();
		}
	}
	
	@Test
	public void testCorruptedDescriptor() throws Exception {
		System.err.println("Start "+getMethodName()+":");
		CorruptionRunner r = null;
		try {
			r = new CorruptionRunner();
			assertTrue(r.db.put(new WriteOptions(), S0("foo"), S0("hello")).ok());
			DBImpl dbi = (DBImpl) (r.db);
			dbi.TEST_CompactMemTable();
			dbi.TEST_CompactRange(0, null, null);

			dbi.TEST_cleanTableCache();
			
			r.corrupt(FileType.DescriptorFile, 0, 1000);
			Status s = r.tryReopen();
			assertTrue(!s.ok());

			r.repairDB();
			r.reopen();
			ByteBuf v = ByteBufFactory.newUnpooled();
			assertTrue(r.db.get(new ReadOptions(), S0("foo"), v).ok());
			assertEquals("hello", v.encodeToString());
		} catch (Exception e) {
			e.printStackTrace();
			assertTrue(false);
		} finally {
			r.delete();
		}
	}
	
	@Test
	public void testCompactionInputError() throws Exception {
		System.err.println("Start "+getMethodName()+":");
		CorruptionRunner r = null;
		try {
			r = new CorruptionRunner();
			r.build(10);
			  DBImpl dbi = (DBImpl)(r.db);
			  dbi.TEST_CompactMemTable();
			  final int last = DBFormat.kMaxMemCompactLevel;
			  assertEquals(1, r.property("leveldb.num-files-at-level" + last));

			  dbi.TEST_cleanTableCache();
			  
			  r.corrupt(FileType.TableFile, 100, 1);
			  r.check(5, 9);

			  // Force compactions by writing lots of values
			  r.build(10000);
			  r.check(10000, 10000);
		} catch (Exception e) {
			e.printStackTrace();
			assertTrue(false);
		} finally {
			r.delete();
		}
	}
	
	@Test
	public void testCompactionInputErrorParanoid() throws Exception {
		System.err.println("Start "+getMethodName()+":");
		CorruptionRunner r = null;
		try {
			r = new CorruptionRunner();
			r.options.paranoidChecks = true;
			r.options.writeBufferSize = 512 << 10;
			r.reopen();
			DBImpl dbi = (DBImpl) (r.db);

			// Make multiple inputs so we need to compact.
			for (int i = 0; i < 2; i++) {
				r.build(10);
				dbi.TEST_CompactMemTable();
				
				dbi.TEST_cleanTableCache();
				
				r.corrupt(FileType.TableFile, 100, 1);
				r.env.sleepForMilliseconds(100);
			}
			dbi.compactRange(null, null);

			// Write must fail because of corrupted table
			ByteBuf tmp1 = ByteBufFactory.newUnpooled();
			ByteBuf tmp2 = ByteBufFactory.newUnpooled();
			Status s = r.db.put(new WriteOptions(), Key(5, tmp1), Value(5, tmp2));
			assertTrue(!s.ok());
		} catch (Exception e) {
			e.printStackTrace();
			assertTrue(false);
		} finally {
			r.delete();
		}
	}

	@Test
	public void testUnrelatedKeys() throws Exception {
		System.err.println("Start "+getMethodName()+":");
		CorruptionRunner r = null;
		try {
			r = new CorruptionRunner();

			r.build(10);
			DBImpl dbi = (DBImpl) (r.db);
			dbi.TEST_CompactMemTable();
			
			dbi.TEST_cleanTableCache();
			
			r.corrupt(FileType.TableFile, 100, 1);

			ByteBuf tmp1 = ByteBufFactory.newUnpooled();
			ByteBuf tmp2 = ByteBufFactory.newUnpooled();
			assertTrue(r.db.put(new WriteOptions(), Key(1000, tmp1), Value(1000, tmp2)).ok());
			ByteBuf v = ByteBufFactory.newUnpooled();
			assertTrue(r.db.get(new ReadOptions(), Key(1000, tmp1), v).ok());
			assertEquals(Value(1000, tmp2).encodeToString(), v.encodeToString());
			dbi.TEST_CompactMemTable();
			assertTrue(r.db.get(new ReadOptions(), Key(1000, tmp1), v).ok());
			assertEquals(Value(1000, tmp2).encodeToString(), v.encodeToString());
		} catch (Exception e) {
			e.printStackTrace();
			assertTrue(false);
		} finally {
			r.delete();
		}
	}

	// Return the ith key
	static Slice Key(int i, ByteBuf storage) {
		String s = String.format("%016d", i);
		storage.assign(s);
		return SliceFactory.newUnpooled(storage);
	}

	// Return the value to associate with the specified key
	static Slice Value(long k, ByteBuf storage) {
		Random0 r = new Random0(k);
		return Utils.randomString(r, kValueSize, storage);
	}
}
