package org.ht.jleveldb.db;

import org.ht.jleveldb.Env;
import org.ht.jleveldb.FileName;
import org.ht.jleveldb.Iterator0;
import org.ht.jleveldb.Options;
import org.ht.jleveldb.RandomAccessFile0;
import org.ht.jleveldb.ReadOptions;
import org.ht.jleveldb.Status;
import org.ht.jleveldb.table.Table;
import org.ht.jleveldb.util.Cache;
import org.ht.jleveldb.util.Coding;
import org.ht.jleveldb.util.Object0;
import org.ht.jleveldb.util.Slice;

public class TableCache {
	
	static class TableAndFile {
		RandomAccessFile0 file;
		Table table;
	};

	static class DeleteEntry implements Cache.Deleter {		
		public void run(Slice key, Object value) {
			TableAndFile tf = (TableAndFile)value;
			if (tf == null)
				return;
			
			tf.table.delete();
			tf.table = null; //delete tf->table;
			tf.file.delete();
			tf.file = null;//delete tf->file; env_poxis.cc
			tf = null; //delete tf;
		}
	}
	
	static class UnrefEntry implements Runnable {
		Cache cache;
		Cache.Handle handle;
		
		public UnrefEntry(Cache cache, Cache.Handle handle) {
			this.cache = cache;
			this.handle = handle;
		}
		public void run() {
			if (cache == null)
				return;
			cache.release(handle);
			cache = null;
			handle = null;
		}
	}

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
            Object0<Table> table0) {
		if (table0 != null)
			table0.setValue(null);

		Object0<Cache.Handle> handle0 = new Object0<Cache.Handle>();
		Status s = findTable(fileNumber, fileSize, handle0);
		if (!s.ok()) {
		    return Iterator0.newErrorIterator(s);
		}
		Cache.Handle handle = handle0.getValue();

		Table table = ((TableAndFile)cache.value(handle)).table;
		Iterator0 result = table.newIterator(options);
		result.registerCleanup(new UnrefEntry(cache, handle));
		if (table0 != null) {
			table0.setValue(table);
		}
		return result;
	}
	
	public Iterator0 newIterator(ReadOptions options,
            long fileNumber,
            long fileSize) {
		return newIterator(options, fileNumber, fileSize, null);
	}
	
	  // If a seek to internal key "k" in specified file finds an entry,
	  // call (*handle_result)(arg, found_key, found_value).
	public Status get(ReadOptions options,
	             long fileNumber,
	             long fileSize,
	             Slice k,
	             Object arg,
	             Table.HandleResult saver) {
		Object0<Cache.Handle> handle0 = new Object0<Cache.Handle>();
		Status s = findTable(fileNumber, fileSize, handle0);
		if (s.ok()) {
			Cache.Handle handle = handle0.getValue();
		    Table t = ((TableAndFile)cache.value(handle)).table;
		    s = t.internalGet(options, k, arg, saver);
		    cache.release(handle);
		}
		return s;
	}
	  
	final static int kUint64Size = 8;
	
	// Evict any entry for the specified file number
	public void evict(long fileNumber) {
		byte buf[] = new byte[kUint64Size];
		Coding.encodeFixedNat64(buf, 0, fileNumber);
		cache.erase(new Slice(buf, 0, kUint64Size));
	}
	
	Env env;
	String dbname;
	Options options;
	Cache cache;
	
	Status findTable(long fileNumber, long fileSize, Object0<Cache.Handle> handle) {
		Status s = Status.ok0();
		byte buf[] = new byte[kUint64Size];
		Coding.encodeFixedNat64(buf, 0, kUint64Size);
		Slice key = new Slice(buf, 0, kUint64Size);
		handle.setValue(cache.lookup(key));
		if (handle.getValue() == null) {
		    String fname = FileName.getTableFileName(dbname, fileNumber);
		    Object0<RandomAccessFile0> file0 = new Object0<RandomAccessFile0>();
		    
		    s = env.newRandomAccessFile(fname, file0);
		    if (!s.ok()) {
		    	String oldFname = FileName.getSSTTableFileName(dbname, fileNumber);
		    	if (env.newRandomAccessFile(oldFname, file0).ok()) {
		    		s = Status.ok0();
		    	}
		    }
		    
		    Object0<Table> table0 = new Object0<Table>();
		    if (s.ok()) {
		    	s = Table.open(options, file0.getValue(), fileSize, table0);
		    }

		    if (!s.ok()) {
		    	assert(table0.getValue() == null);
		    	if (file0.getValue() != null) {
		    		file0.getValue().delete();
		    	}
		    	// We do not cache error results so that if the error is transient,
		    	// or somebody repairs the file, we recover automatically.
		    } else {
		    	TableAndFile tf = new TableAndFile();
		    	tf.file = file0.getValue();
		    	tf.table = table0.getValue();
		    	handle.setValue(cache.insert(key, tf, 1, new DeleteEntry()));
		    }
		}
		return s;
	}
	

}
