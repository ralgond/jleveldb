package org.ht.jleveldb;

import org.ht.jleveldb.util.BloomFilterPolicy;
import org.ht.jleveldb.util.ByteBuf;
import org.ht.jleveldb.util.Slice;

public abstract class FilterPolicy {
	// Return the name of this policy.  Note that if the filter encoding
	  // changes in an incompatible way, the name returned by this method
	  // must be changed.  Otherwise, old incompatible filters may be
	  // passed to methods of this type.
	public abstract String name();
		
	// keys[0,n-1] contains a list of keys (potentially with duplicates)
	  // that are ordered according to the user supplied comparator.
	  // Append a filter that summarizes keys[0,n-1] to *dst.
	  //
	  // Warning: do not change the initial contents of *dst.  Instead,
	  // append the newly constructed filter to *dst.
	public abstract void createFilter(Slice[] keys, int n, ByteBuf buf);
		
	// "filter" contains the data appended by a preceding call to
	  // CreateFilter() on this class.  This method must return true if
	  // the key was in the list of keys passed to CreateFilter().
	  // This method may return true or false if the key was not on the
	  // list, but it should aim to return false with a high probability.
	public abstract boolean keyMayMatch(Slice key, Slice filter);
		
	// Return a new filter policy that uses a bloom filter with approximately
	// the specified number of bits per key.  A good value for bits_per_key
	// is 10, which yields a filter with ~ 1% false positive rate.
	//
	// Callers must delete the result after any database that is using the
	// result has been closed.
	//
	// Note: if you are using a custom comparator that ignores some parts
	// of the keys being compared, you must not use NewBloomFilterPolicy()
	// and must provide your own FilterPolicy that also ignores the
	// corresponding parts of the keys.  For example, if the comparator
	// ignores trailing spaces, it would be incorrect to use a
	// FilterPolicy (like NewBloomFilterPolicy) that does not ignore
	// trailing spaces in keys.
	public static FilterPolicy newBloomFilterPolicy(int bitsPerKey) {
		return new BloomFilterPolicy(bitsPerKey);
	}
}
