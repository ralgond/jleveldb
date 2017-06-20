package org.ht.jleveldb.db.format;

import org.ht.jleveldb.util.ByteBuf;
import org.ht.jleveldb.util.Slice;

public class ParsedInternalKey {
	public Slice userKey;
	public long sequence;
	public ValueType type;

	public ParsedInternalKey() {
		// Intentionally left uninitialized (for speed)
	}  
	
	public ParsedInternalKey(Slice u, long seq, ValueType t) {
		userKey = u;
		sequence = seq;
		type = t;
	}
	
	// Return the length of the encoding of "key".
	final public int encodingLength() {
		return userKey.size() + 8;
	}
	
	// Append the serialization of "key" to buf.
	public void append(ByteBuf buf) {
		
	}
	
	// Attempt to parse an internal key from "internal_key".  On success,
	// stores the parsed data in "*result", and returns true.
	//
	// On error, returns false, leaves "*result" in an undefined state.
	public boolean parse(Slice internalKey) {
		int n = internalKey.size();
		if (n < 8) 
			return false;
		
		long num = 0;// = DecodeFixed64(internal_key.data() + n - 8);
	    byte c = (byte)(num & 0xff);
	    sequence = num >> 8;
		if (c == ValueType.Deletion.type()) {
			type = ValueType.Deletion;
		} else if (c == ValueType.Value.type()) {
			type = ValueType.Value;
		} else {
			return false;
		}
	    userKey = new Slice(internalKey.data(), 8, n - 8);
	    
	    return true;
	}
	
	public String debugString() {
		//TODO
		return null;
	}
};
