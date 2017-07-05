package com.tchaicatkovsky.jleveldb.test;

import com.tchaicatkovsky.jleveldb.DB;
import com.tchaicatkovsky.jleveldb.FileName;
import com.tchaicatkovsky.jleveldb.FileType;
import com.tchaicatkovsky.jleveldb.Iterator0;
import com.tchaicatkovsky.jleveldb.LevelDB;
import com.tchaicatkovsky.jleveldb.Options;
import com.tchaicatkovsky.jleveldb.ReadOptions;
import com.tchaicatkovsky.jleveldb.Status;
import com.tchaicatkovsky.jleveldb.WriteBatch;
import com.tchaicatkovsky.jleveldb.WriteOptions;
import com.tchaicatkovsky.jleveldb.util.ByteBuf;
import com.tchaicatkovsky.jleveldb.util.ByteBufFactory;
import com.tchaicatkovsky.jleveldb.util.Cache;
import com.tchaicatkovsky.jleveldb.util.Long0;
import com.tchaicatkovsky.jleveldb.util.Object0;
import com.tchaicatkovsky.jleveldb.util.Random0;
import com.tchaicatkovsky.jleveldb.util.Slice;
import com.tchaicatkovsky.jleveldb.util.SliceFactory;
import com.tchaicatkovsky.jleveldb.util.TestUtil;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;

public class TestCorruption {

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
		    dbname = TestUtil.tmpDir() + "/corruption_test";
		    LevelDB.destroyDB(dbname, options);

		    db = null;
		    options.createIfMissing = true;
		    reopen();
		    options.createIfMissing = false;
		}
		
		public void delete() throws Exception {
		     db.close();
		     LevelDB.destroyDB(dbname, new Options());
		     tinyCache = null;
		}
		
		public Status tryReopen() throws Exception {
		    db.close();
		    db = null;
		    
		    Object0<DB> db0 = new Object0<>();
		    Status s = LevelDB.newDB(options, dbname, db0);
		    db = db0.getValue();
		    return s;
		}

		public void reopen() throws Exception {
		    assertTrue(tryReopen().ok());
		}

		public void repairDB() throws Exception {
		    db.close();
		    db = null;
		    assertTrue(LevelDB.repairDB(dbname, options).ok());
		}
		
		// Return the ith key
		Slice Key(int i, ByteBuf storage) {
		    String s = String.format("%016d", i);
		    storage.assign(s);
		    return SliceFactory.newUnpooled(storage);
		}

		  // Return the value to associate with the specified key
		Slice Value(long k, ByteBuf storage) {
		    Random0 r = new Random0(k);
		    return TestUtil.randomString(r, kValueSize, storage);
		}
		
		public void build(int n) throws Exception {
		    ByteBuf key_space = ByteBufFactory.newUnpooled();
		    ByteBuf value_space = ByteBufFactory.newUnpooled();
		    
		    WriteBatch batch = new WriteBatch();
		    for (int i = 0; i < n; i++) {
		    	//if ((i % 100) == 0) fprintf(stderr, "@ %d of %d\n", i, n);
		    	Slice key = Key(i, key_space);
		    	batch.clear();
		    	batch.put(key, Value(i, value_space));
		    	WriteOptions options = new WriteOptions();
		    	// Corrupt() doesn't work without this sync on windows; stat reports 0 for
		    	// the file size.
		    	if (i == n - 1) {
		    		options.sync = true;
		    	}
		    	assertTrue(db.write(options, batch).ok());
		    }
		}
		
		public void check(int min_expected, int max_expected) {
		    long next_expected = 0;
		    int missed = 0;
		    int bad_keys = 0;
		    int bad_values = 0;
		    int correct = 0;
		    ByteBuf value_space = ByteBufFactory.newUnpooled(); 
		    Iterator0 iter = db.newIterator(new ReadOptions());
		    for (iter.seekToFirst(); iter.valid(); iter.next()) {
		    	String ins = iter.key().encodeToString();
		    	if (ins.equals("") || ins.equals("~")) {
		    		// Ignore boundary keys.
		    		continue;
		    	}
		    	Object0<String> ins0 = new Object0<>(ins);
		    	Long0 key0 = new Long0();
		    	if (!FileName.consumeDecimalNumber(ins0, key0) || 
		    			!ins0.getValue().isEmpty() || 
		    			key0.getValue() < next_expected) {
		    		bad_keys++;
		    		continue;
		    	}
		    	missed += (key0.getValue() - next_expected);
		    	next_expected = key0.getValue() + 1;
		    	if (!iter.value().equals(Value(key0.getValue(), value_space))) {
		    		bad_values++;
		    	} else {
		    		correct++;
		    	}
		    }
		    iter.delete();
		    iter = null;

		    System.err.printf("expected=%d..%d; got=%d; bad_keys=%d; bad_values=%d; missed=%d\n",
		            min_expected, max_expected, correct, bad_keys, bad_values, missed);
		    assertTrue(min_expected <= correct);
		    assertTrue(max_expected >= correct);
		}
		
		public void corrupt(FileType filetype, int offset, int bytes_to_corrupt) {
			    // Pick file to corrupt
			ArrayList<String> filenames = new ArrayList<>();
			assertTrue(env.getChildren(dbname, filenames).ok());
			Long0 number0 = new Long0();
			Object0<FileType> type0 = new Object0<>();
			String fname = "";
			int picked_number = -1;
			for (int i = 0; i < filenames.size(); i++) {
				if (FileName.parseFileName(filenames.get(i), number0, type0) &&
			          type0.getValue() == filetype &&
			          (int)number0.getValue() > picked_number) {  
					// Pick latest file
			        fname = dbname + "/" + filenames.get(i);
			        picked_number = (int)number0.getValue();
				}
			}
			assertTrue(!fname.isEmpty());

//			    struct stat sbuf;
//			    if (stat(fname.c_str(), &sbuf) != 0) {
//			      const char* msg = strerror(errno);
//			      ASSERT_TRUE(false) << fname << ": " << msg;
//			    }
//
//			    if (offset < 0) {
//			      // Relative to end of file; make it absolute
//			      if (-offset > sbuf.st_size) {
//			        offset = 0;
//			      } else {
//			        offset = sbuf.st_size + offset;
//			      }
//			    }
//			    if (offset > sbuf.st_size) {
//			      offset = sbuf.st_size;
//			    }
//			    if (offset + bytes_to_corrupt > sbuf.st_size) {
//			      bytes_to_corrupt = sbuf.st_size - offset;
//			    }
//
//			    // Do it
//			    std::string contents;
//			    Status s = ReadFileToString(Env::Default(), fname, &contents);
//			    ASSERT_TRUE(s.ok()) << s.ToString();
//			    for (int i = 0; i < bytes_to_corrupt; i++) {
//			      contents[i + offset] ^= 0x80;
//			    }
//			    s = WriteStringToFile(Env::Default(), contents, fname);
//			    ASSERT_TRUE(s.ok()) << s.ToString();
//			  }
		}
	}
}
