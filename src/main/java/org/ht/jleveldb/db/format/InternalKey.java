package org.ht.jleveldb.db.format;

import org.ht.jleveldb.util.ByteBuf;
import org.ht.jleveldb.util.Slice;

/**
 * Modules in this directory should keep internal keys wrapped inside
 * the following class instead of plain strings so that we do not
 * incorrectly use string comparisons instead of an InternalKeyComparator.
 * 
 * @author Teng Huang ht201509@163.com
 */
public class InternalKey {
	ByteBuf rep;
	
	public InternalKey() {
		
	}
	
	public ByteBuf rep() {
		return rep;
	}
	
	public InternalKey(Slice userKey, long s, ValueType t) {
		(new ParsedInternalKey(userKey, s, t)).append(rep);
	}
	
	public void decodeFrom(Slice s) {
		rep.assign(s.data(), s.offset(), s.size());
	}
	
	public void decodeFrom(ByteBuf b) {
		rep.assign(b.data(), b.size());
	}
	
	//TODO: reduce the copy frequency.
	public Slice encode() {
		//TODO
		return new Slice(rep);
	}
	
	public Slice userKey() {
		//TODO
		return DBFormat.extractUserKey(new Slice(rep));
	}
	
	public void setFrom(ParsedInternalKey p) {
		rep.clear();
		p.append(rep);
	}
	
	public void clear() {
		rep.clear();
	}
	
	@Override
	public String toString() {
		return null;
	}
	
	public void assgin(InternalKey ik) {
		rep = ik.rep;
	}
	
	public String debugString() {
		//TODO
		return null;
	}
}
