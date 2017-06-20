package org.ht.jleveldb.db;

import java.util.Map;
import java.util.TreeMap;

import org.ht.jleveldb.Iterator0;
import org.ht.jleveldb.Status;
import org.ht.jleveldb.db.format.InternalKeyComparator;
import org.ht.jleveldb.db.format.LookupKey;
import org.ht.jleveldb.db.format.ValueType;
import org.ht.jleveldb.util.ByteBuf;
import org.ht.jleveldb.util.Slice;

public class MemTable {
	public MemTable(InternalKeyComparator c) {
		this.comparator = new KeyComparator(c);
	}
	
	
	static class KeyComparator {
		InternalKeyComparator comparator;
		public KeyComparator(InternalKeyComparator comparator) {
			this.comparator = comparator;
		}
		
	    public int operator(ByteBuf a, ByteBuf b) {
	    	//TODO
	    	return 0;
	    }
	}
	
	Map<ByteBuf, ByteBuf> table = new TreeMap<ByteBuf, ByteBuf>();
	
	KeyComparator comparator;
	
	
	// Returns an estimate of the number of bytes of data in use by this
	  // data structure. It is safe to call when MemTable is being modified.
	public long approximateMemoryUsage() {
		return 0;
	}
	  
	  
	  // Return an iterator that yields the contents of the memtable.
	  //
	  // The caller must ensure that the underlying MemTable remains live
	  // while the returned iterator is live.  The keys returned by this
	  // iterator are internal keys encoded by AppendInternalKey in the
	  // db/format.{h,cc} module.
	public Iterator0 newIterator() {
		//TODO
		return null;
	}
	
	  // Add an entry into memtable that maps key to value at the
	  // specified sequence number and with the specified type.
	  // Typically value will be empty if type==kTypeDeletion.
	public void add(long seq, ValueType type, Slice key, Slice value) {
		//TODO
	}
	
	
	  // If memtable contains a value for key, store it in *value and return true.
	  // If memtable contains a deletion for key, store a NotFound() error
	  // in *status and return true.
	  // Else, return false.
	public boolean get(LookupKey key, ByteBuf value, Status s) {
		//TODO
		return false;
	}
}
