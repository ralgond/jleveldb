package org.ht.jleveldb.db.format;

import org.ht.jleveldb.util.ByteBuf;
import org.ht.jleveldb.util.ByteBufFactory;
import org.ht.jleveldb.util.Coding;
import org.ht.jleveldb.util.Comparator0;
import org.ht.jleveldb.util.Slice;
import org.ht.jleveldb.util.Strings;

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
	
	/**
	 * Order by:
	 * 	increasing user key (according to user-supplied comparator)
	 * 	decreasing sequence number
	 * 	decreasing type (though sequence# should be enough to disambiguate)
	 * 
	 * 	aikey, bikey : {userKey:byte[size-8], seq_type:[8]}
	 */
//	public int compare(Slice aikey, Slice bikey) {
//		int r = userComparator.compare(aikey.data, aikey.offset, aikey.size()-8, 
//				bikey.data, bikey.offset, bikey.size()-8);
//		if (r == 0) {
//		    long anum = Coding.decodeFixedNat64(aikey.data, aikey.limit - 8);
//		    long bnum = Coding.decodeFixedNat64(bikey.data, bikey.limit - 8);
//		    r = anum > bnum ? -1 : +1;
//		    //System.out.println("anum="+anum+", bnum="+bnum+", r="+r);
//		}
//		return r;
//	}
	
	public int compare(byte[] a, int aoff, int asize, byte[] b, int boff, int bsize) {
//		Slice a0 = new Slice(a,aoff,asize-8);
//		Slice b0 = new Slice(b,boff,bsize-8);
//		System.out.printf("InternalKeyComparator, a0.size=%d\n", a0.size());
//		if (asize > 0)
//			System.out.printf("InternalKeyComparator, a[0]=%d, aoff=%d, asize=%d\n", (int)a[aoff+0], aoff, asize);
//		if (asize > 1)
//			System.out.printf("InternalKeyComparator, a[1]=%d, aoff=%d, asize=%d\n", (int)a[aoff+1], aoff, asize);
		
		int r = userComparator.compare(a, aoff, asize-8, b, boff, bsize-8);
		
//		System.out.printf("InternalKeyComparator, a=%s, b=%s, r=%d, cmp.class=%s\n",
//				Strings.escapeString(a0), Strings.escapeString(b0), r, userComparator.getClass().getName());
//		if (asize > 0)
//			System.out.printf("InternalKeyComparator, a[0]=%d\n", a[aoff+0]&0xff);
//		if (bsize > 0)
//			System.out.printf("InternalKeyComparator, b[0]=%d\n", b[boff+0]&0xff);
		
		if (r == 0) {
		    long anum = Coding.decodeFixedNat64(a, aoff + asize - 8);
		    long bnum = Coding.decodeFixedNat64(b, boff + bsize - 8);
		    r = anum > bnum ? -1 : +1;
		    //System.out.println("anum="+anum+", bnum="+bnum+", r="+r);
		}
		return r;
	}
	
	public int compare(InternalKey a, InternalKey b) {
		//InternalKey a => {rep:ByteBuf}
		return compare(a.encode(), b.encode());
	}
	
	public String name() {
		return "leveldb.InternalKeyComparator";
	}
	
	/**
	 * @param start [INPUT][OUTPUT]
	 */
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
	
	/**
	 * @param key [INPUT][OUTPUT]
	 */
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
	
	public final static long packSequenceAndType(long seq, ValueType t) {
		assert(seq <= DBFormat.kMaxSequenceNumber);
		assert(t.type <= DBFormat.kValueTypeForSeek.type);
		return (seq << 8) | (t.type & 0xff);
	}
}
