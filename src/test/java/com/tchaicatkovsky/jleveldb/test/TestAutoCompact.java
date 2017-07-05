package com.tchaicatkovsky.jleveldb.test;

import org.junit.Test;

import com.tchaicatkovsky.jleveldb.CompressionType;
import com.tchaicatkovsky.jleveldb.DB;
import com.tchaicatkovsky.jleveldb.Iterator0;
import com.tchaicatkovsky.jleveldb.LevelDB;
import com.tchaicatkovsky.jleveldb.Options;
import com.tchaicatkovsky.jleveldb.Range;
import com.tchaicatkovsky.jleveldb.ReadOptions;
import com.tchaicatkovsky.jleveldb.WriteOptions;
import com.tchaicatkovsky.jleveldb.db.DBImpl;
import com.tchaicatkovsky.jleveldb.util.Cache;
import com.tchaicatkovsky.jleveldb.util.UnpooledSlice;
import com.tchaicatkovsky.jleveldb.util.Object0;
import com.tchaicatkovsky.jleveldb.util.Slice;
import com.tchaicatkovsky.jleveldb.util.TestUtil;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;

public class TestAutoCompact {
	
	static  int kValueSize = 200 * 1024;
	static  int kTotalSize = 100 * 1024 * 1024;
	static  int kCount = kTotalSize / kValueSize;
	
	static class AutoCompactRunner {
		String dbname;
		Cache tinyCache;
		Options options = new Options();
		DB db;
		
		public AutoCompactRunner() throws Exception {
			dbname = TestUtil.tmpDir() + "/autocompact_test";
		    tinyCache = Cache.newLRUCache(100);
		    options.blockCache = tinyCache;
		    LevelDB.destroyDB(dbname, options);
		    options.createIfMissing = true;
		    options.compression = CompressionType.kNoCompression;
		    Object0<DB> db0 = new Object0<>();
		    assertTrue(LevelDB.newDB(options, dbname, db0).ok());
		    db = db0.getValue();
		}
		
		public void delete() throws Exception {
			db.close();
			db = null;
			LevelDB.destroyDB(dbname, options);
			tinyCache = null;
		}
		
		public String key(int i) {
		    return String.format("key%06d", i);
		}
		
		public long size(Slice start, Slice limit) {
		    Range r = new Range(start, limit);
		    ArrayList<Long> sizes = new ArrayList<>();
		    ArrayList<Range> l = new ArrayList<>();
		    l.add(r);
		    db.getApproximateSizes(l, sizes);
		    return sizes.get(0);
		}
		
		public long size(String start, String limit) {
			return size(new UnpooledSlice(start), new UnpooledSlice(limit));
		}
		

		// Read through the first n keys repeatedly and check that they get
		// compacted (verified by checking the size of the key space).
		public void DoReads(int n) throws Exception {
			String value = TestUtil.makeString(kValueSize, 'x');
			DBImpl dbi = (DBImpl)(db);

			// Fill database
			for (int i = 0; i < kCount; i++) {
			    assertTrue(db.put(WriteOptions.defaultOne(), new UnpooledSlice(key(i)), new UnpooledSlice(value)).ok());
			}
			assertTrue(dbi.TEST_CompactMemTable().ok());

			  // Delete everything
			for (int i = 0; i < kCount; i++) {
				assertTrue(db.delete(WriteOptions.defaultOne(), new UnpooledSlice(key(i))).ok());
			}
			assertTrue(dbi.TEST_CompactMemTable().ok());

			  // Get initial measurement of the space we will be reading.
			long initial_size = size(key(0), key(n));
			long initial_other_size = size(key(n), key(kCount));

			  // Read until size drops significantly.
			String limit_key = key(n);
			for (int read = 0; true; read++) {
				assertTrue(read  < 100);
			    Iterator0 iter = db.newIterator(new ReadOptions());
			    for (iter.seekToFirst();
			         iter.valid() && iter.key().encodeToString().compareTo(limit_key) < 0;
			         iter.next()) {
			    	// Drop data
			    }
			    iter.delete();
			    iter = null;
			    // Wait a little bit to allow any triggered compactions to complete.
			    LevelDB.defaultEnv().sleepForMilliseconds(1000);
			    long size = size(key(0), key(n));
			    System.err.printf("iter %3d => %7.3f MB [other %7.3f MB]\n",
			            read+1, size/1048576.0, size(key(n), key(kCount))/1048576.0);
			    if (size <= initial_size/10) {
			    	break;
			    }
			}

			// Verify that the size of the key space not touched by the reads
			// is pretty much unchanged.
			long final_other_size = size(key(n), key(kCount));
			assertTrue(final_other_size <= initial_other_size + 1048576);
			assertTrue(final_other_size >= initial_other_size/5 - 1048576);
		}
	}
	
	@Test
	public void testReadAll() throws Exception {
		AutoCompactRunner r = new AutoCompactRunner();
		r.DoReads(kCount);
		r.delete();
	}
	
	@Test
	public void testReadHalf() throws Exception {
		AutoCompactRunner r = new AutoCompactRunner();
		r.DoReads(kCount / 2);
		r.delete();
	}
}
