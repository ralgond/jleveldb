package org.ht.jleveldb.util;

public abstract class Comparator0 {
	
	public abstract int compare(byte[] a, int aoff, int asize, byte[] b, int boff, int bsize);
	
	public int compare(Slice a, ByteBuf b) {
		return compare(a.data, a.offset, a.size(), b.data(), 0, b.size());
	}
	
	public int compare(ByteBuf a, Slice b) {
		return compare(a.data(), 0, a.size(), b.data, b.offset, b.size());
	}
	
	public int compare(ByteBuf a, ByteBuf b) {
		return compare(a.data(), 0, a.size(), b.data(), 0, b.size());
	}
	
	/**
	 * Three-way comparison.  Returns value:</br>
	 * <  0 iff "a" < "b",</br>
	 * == 0 iff "a" == "b",</br>
	 * >  0 iff "a" > "b"</br>
	 * @param a
	 * @param b
	 * @return
	 */
	public int compare(Slice a, Slice b) {
		return compare(a.data, a.offset, a.size(), b.data, b.offset, b.size());
	}
	
	/**
	 * The name of the comparator.  Used to check for comparator 
	 * mismatches (i.e., a DB created with one comparator is 
	 * accessed using a different comparator.</br></br>
	 * 
	 * The client of this package should switch to a new name whenever
	 * the comparator implementation changes in a way that will cause
	 * the relative ordering of any two keys to change.</br></br>
	 * 
	 * Names starting with "leveldb." are reserved and should not be used 
	 * by any clients of this package.
	 * 
	 * @return
	 */
	public abstract String name();
		
	/**
	 * Advanced functions: these are used to reduce the space requirements
	 * for internal data structures like index blocks.</br></br>
	 * 
	 * If {@code start < limit}, changes {@code start} to a short string in {@code [start,limit)}.</br>
	 * Simple comparator implementations may return with start unchanged,
	 * i.e., an implementation of this method that does nothing is correct.
	 * @param start [INPUT][OUTPUT]
	 * @param limit
	 */
	public abstract void findShortestSeparator(ByteBuf start, Slice limit);
		
	/**
	 * Changes {@code key} to a short string >= {@code key}.</br>
	 * Simple comparator implementations may return with {@code key} unchanged,
	 * i.e., an implementation of this method that does nothing is correct.
	 * @param key [INPUT][OUTPUT]
	 */
	public abstract void findShortSuccessor(ByteBuf key);
		
	public static Comparator0 bytewiseComparator() {
		return BytewiseComparatorImpl.getInstance();
	}
}

