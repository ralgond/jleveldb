package org.ht.jleveldb.db.format;

import org.ht.jleveldb.util.ByteBuf;
import org.ht.jleveldb.util.ByteBufFactory;
import org.ht.jleveldb.util.Coding;
import org.ht.jleveldb.util.Comparator0;
import org.ht.jleveldb.util.Slice;

/**
 * A comparator for internal keys that uses a specified comparator for
 * the user key portion and breaks ties by decreasing sequence number.
 * 
 * @author Teng Huang ht201509@163.com
 */
public class InternalKeyComparator extends Comparator0 {

	Comparator0 userComparator;
	
	public InternalKeyComparator(Comparator0 userComparator) {
		this.userComparator = userComparator;
	}
	
	public int compare(Slice akey, Slice bkey) {
		// Order by:
		//    increasing user key (according to user-supplied comparator)
		//    decreasing sequence number
		//    decreasing type (though sequence# should be enough to disambiguate)
		int r = userComparator.compare(DBFormat.extractUserKey(akey), DBFormat.extractUserKey(bkey));
		if (r == 0) {
		    long anum = Coding.decodeFixedNat64(akey.data, akey.limit - 8);
		    long bnum = Coding.decodeFixedNat64(bkey.data, bkey.limit - 8);
		    if (anum > bnum) {
		    	r = -1;
		    } else if (anum < bnum) {
		    	r = +1;
		    }
		}
		return r;
	}
	
	public int compare(byte[] a, int aoff, int asize, byte[] b, int boff, int bsize) {
		throw new UnsupportedOperationException();
	}
	
	public int compare(InternalKey a, InternalKey b) {
		return compare(a.encode(), b.encode());
	}
	
	public String name() {
		return "leveldb.InternalKeyComparator";
	}
	
	public void findShortestSeparator(ByteBuf start, Slice limit) {
		// Attempt to shorten the user portion of the key
		Slice userStart = DBFormat.extractUserKey(new Slice(start));
		Slice userLimit = DBFormat.extractUserKey(limit);
		ByteBuf tmp = ByteBufFactory.defaultByteBuf();
		tmp.assign(userStart.data(), userStart.size());
		userComparator.findShortestSeparator(tmp, userLimit);
		if (tmp.size() < userStart.size() &&
		    userComparator.compare(userStart, new Slice(tmp)) < 0) {
			// User key has become shorter physically, but larger logically.
		    // Tack on the earliest possible number to the shortened user key.
			tmp.writeFixedNat64(packSequenceAndType(DBFormat.kMaxSequenceNumber,DBFormat.kValueTypeForSeek));
		    assert(this.compare(new Slice(start), new Slice(tmp)) < 0);
		    assert(this.compare(new Slice(tmp), limit) < 0);
		    start.swap(tmp);
		}
	}
	
	public void findShortSuccessor(ByteBuf key) {
		Slice userKey = DBFormat.extractUserKey(new Slice(key));
		ByteBuf tmp = ByteBufFactory.defaultByteBuf();
		tmp.assign(userKey.data(), userKey.size());
		userComparator.findShortSuccessor(tmp);
		if (tmp.size() < userKey.size() &&
		      userComparator.compare(userKey, new Slice(tmp)) < 0) {
		    // User key has become shorter physically, but larger logically.
		    // Tack on the earliest possible number to the shortened user key.
			tmp.writeFixedNat64(packSequenceAndType(DBFormat.kMaxSequenceNumber,DBFormat.kValueTypeForSeek));
		    assert(this.compare(new Slice(key), new Slice(tmp)) < 0);
		    key.swap(tmp);
		}
	}
	
	public Comparator0 userComparator() {
		return userComparator;
	}
	
	static long packSequenceAndType(long seq, ValueType t) {
		assert(seq <= DBFormat.kMaxSequenceNumber);
		assert(t.type <= DBFormat.kValueTypeForSeek.type);
		return (seq << 8) | (t.type & 0xff);
	}
}
