package org.ht.jleveldb.db.format;

import org.ht.jleveldb.util.Slice;

/**
 * A helper class useful for DBImpl::Get()
 * 
 * @author Teng Huang ht201509@163.com
 */
public class LookupKey {
	
	  // We construct a char array of the form:
	  //    klength  varint32               <-- start_
	  //    userkey  char[klength]          <-- kstart_
	  //    tag      uint64
	  //                                    <-- end_
	  // The array is a suitable MemTable key.
	  // The suffix starting with "userkey" can be used as an InternalKey.	
	byte[] data;
    int start;
    int kstart;
    int end;
    byte space[];      // Avoid allocation for short keys
	  
	public LookupKey(Slice userKey, long sequence) {
		
	}
	
	// Return a key suitable for lookup in a MemTable.
	public Slice memtableKey() { 
		return new Slice(data, start, end - start); 
	}
	
	// Return an internal key (suitable for passing to an internal iterator)
	public Slice internalKey() { 
		return new Slice(data, kstart, end - kstart); 
	}
	
	// Return the user key
	public Slice userKey() { 
		return new Slice(data, kstart, end - kstart - 8); 
	}
	
	
}
