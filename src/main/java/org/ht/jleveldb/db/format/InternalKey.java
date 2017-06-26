package org.ht.jleveldb.db.format;

import org.ht.jleveldb.util.ByteBuf;
import org.ht.jleveldb.util.ByteBufFactory;
import org.ht.jleveldb.util.Slice;
import org.ht.jleveldb.util.Strings;

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
		rep = ByteBufFactory.defaultByteBuf();
	}
	
	public ByteBuf rep() {
		return rep;
	}
	
	protected static void appendInternalKey(ByteBuf result, ParsedInternalKey key) {
		result.append(key.userKey.data(), key.userKey.size());
		result.writeFixedNat64(DBFormat.packSequenceAndType(key.sequence, key.type));
	}
	
	public InternalKey(Slice userKey, long s, ValueType t) {
		this();
		appendInternalKey(rep, new ParsedInternalKey(userKey, s, t));
	}
	
	public void decodeFrom(Slice s) {
		rep.assign(s.data(), s.offset(), s.size());
	}
	
	public void decodeFrom(ByteBuf b) {
		rep.assign(b.data(), b.size());
	}
	
	public Slice encode() {
		return new Slice(rep);
	}
	
	public Slice userKey() {
		return DBFormat.extractUserKey(new Slice(rep));
	}
	
	public void setFrom(ParsedInternalKey p) {
		rep.clear();
		DBFormat.appendInternalKey(rep, p);
	}
	
	public void clear() {
		rep.clear();
	}
	
	@Override
	public String toString() {
		return null;
	}
	
	public void assgin(InternalKey ik) {
		rep.assign(ik.rep);
	}
	
	public String debugString() {
		String result;
		ParsedInternalKey parsed = new ParsedInternalKey();
		if (parsed.parse(new Slice(rep))) {
			result = parsed.debugString();
		} else {
		    result = "(bad)";
		    result += Strings.escapeString(rep);
		}
		return result;
	}
	
	@Override
	public InternalKey clone() {
		InternalKey ik = new InternalKey();
		ik.rep.assign(rep);
		return ik;
	}
}
