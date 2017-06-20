package org.ht.jleveldb.db;

import org.ht.jleveldb.Env;
import org.ht.jleveldb.Iterator0;
import org.ht.jleveldb.Options;
import org.ht.jleveldb.RandomAccessFile0;
import org.ht.jleveldb.ReadOptions;
import org.ht.jleveldb.Status;
import org.ht.jleveldb.table.Table;
import org.ht.jleveldb.util.Cache;
import org.ht.jleveldb.util.FuncOutput;
import org.ht.jleveldb.util.Slice;

public class TableCache {
	  // Return an iterator for the specified file number (the corresponding
	  // file length must be exactly "file_size" bytes).  If "tableptr" is
	  // non-NULL, also sets "*tableptr" to point to the Table object
	  // underlying the returned iterator, or NULL if no Table object underlies
	  // the returned iterator.  The returned "*tableptr" object is owned by
	  // the cache and should not be deleted, and is valid for as long as the
	  // returned iterator is live.
	public Iterator0 newIterator(ReadOptions options,
            long fileNumber,
            long fileSize,
            FuncOutput<Table> table) {
		//TODO
		return null;
	}
	
	public Iterator0 newIterator(ReadOptions options,
            long fileNumber,
            long fileSize) {
		return newIterator(options, fileNumber, fileSize, null);
	}
	
	public interface HandleResult {
		public void run(Object arg, Slice foundKey, Slice foundValue);
	}
	
	  // If a seek to internal key "k" in specified file finds an entry,
	  // call (*handle_result)(arg, found_key, found_value).
	public Status get(ReadOptions options,
	             long fileNumber,
	             long fileSize,
	             Slice k,
	             Object arg,
	             HandleResult handle) {
		//TODO
		return null;
	}
	  
	
	// Evict any entry for the specified file number
	public void evict(long fileNumber) {
		//TODO
	}
	
	
	Env env;
	String dbname;
	Options options;
	Cache cache;
	
	Status findTable(long fileNumber, long fileSize, FuncOutput<Cache.Handle> handle) {
		//TODO
		return null;
	}
	
	static class TableAndFile {
		RandomAccessFile0 file;
		Table table;
	};

	public static void deleteEntry(Slice key, Object value) {
		  TableAndFile tf = (TableAndFile)value;
		  tf.table = null; //delete tf->table;
		  tf.file = null;//delete tf->file; env_poxis.cc
		  tf = null; //delete tf;
	}
}
