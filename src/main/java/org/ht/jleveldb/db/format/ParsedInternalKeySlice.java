package org.ht.jleveldb.db.format;

import org.ht.jleveldb.util.Slice;

public class ParsedInternalKeySlice extends Slice {
	public long sequence;
	public ValueType valueType;
	
	public ParsedInternalKeySlice(long seq, ValueType type, byte[] data, int offset, int size) {
		super(data, offset, size);
		sequence = seq;
		valueType = type;
	}
	
	@Override
	public String toString() {
		return String.format("{key:%s,seq:%d,vtype:%d,size:%d}", 
				super.toString(), sequence, valueType.type, size());
	}
}
