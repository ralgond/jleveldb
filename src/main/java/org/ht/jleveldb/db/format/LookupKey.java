package org.ht.jleveldb.db.format;

import org.ht.jleveldb.util.ByteBuf;
import org.ht.jleveldb.util.ByteBufFactory;
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
    long sequence;
    
	public LookupKey(Slice userKey, long sequence) {
		ByteBuf buf = ByteBufFactory.defaultByteBuf(); //TODO(optimize): reduce the frequency of memory allocation.
		buf.require(1);
		
		start = 0;
		int usize = userKey.size();
		buf.writeVarNat32(usize + 8);
		kstart = buf.position();
		buf.append(userKey.data(), usize);
		buf.writeFixedNat64(DBFormat.packSequenceAndType(sequence, DBFormat.kValueTypeForSeek));
		end = buf.position();
		
		data = buf.data();
		
		this.sequence = sequence;
	}
	
	/** 
	 * Return a key suitable for lookup in a MemTable.
	 * @return
	 */
	public ParsedInternalKeySlice memtableKey() { 
		return new ParsedInternalKeySlice(sequence, DBFormat.kValueTypeForSeek, data, kstart, end - kstart - 8); 
	}
	
	/**
	 * Return an internal key (suitable for passing to an internal iterator)
	 * @return
	 */
	public Slice internalKey() { 
		return new Slice(data, kstart, end - kstart); 
	}
	
	/**
	 *  Return the user key
	 * @return
	 */
	public Slice userKey() { 
		return new Slice(data, kstart, end - kstart - 8); 
	}
	
	
}
