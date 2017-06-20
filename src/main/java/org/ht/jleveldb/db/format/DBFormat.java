package org.ht.jleveldb.db.format;

import org.ht.jleveldb.util.Slice;

public class DBFormat {

	public static final int NumLevels = 7;
	
	// Level-0 compaction is started when we hit this many files.
	public static final int L0_CompactionTrigger = 4;
	
	// Soft limit on number of level-0 files.  We slow down writes at this point.
	public static final int L0_SlowdownWritesTrigger = 8;
	
	// Maximum number of level-0 files.  We stop writes at this point.
	public static final int L0_StopWritesTrigger = 12;

	// Maximum level to which a new compacted memtable is pushed if it
	// does not create overlap.  We try to push to level 2 to avoid the
	// relatively expensive level 0=>1 compactions and to avoid some
	// expensive manifest file operations.  We do not push all the way to
	// the largest level since that can generate a lot of wasted disk
	// space if the same key space is being repeatedly overwritten.
	public static final int MaxMemCompactLevel = 2;
	
	// Approximate gap in bytes between samples of data read during iteration.
	public static final int ReadBytesPeriod = 1048576;
	
	// kValueTypeForSeek defines the ValueType that should be passed when
	// constructing a ParsedInternalKey object for seeking to a particular
	// sequence number (since we sort sequence numbers in decreasing order
	// and the value type is embedded as the low 8 bits in the sequence
	// number in internal keys, we need to use the highest-numbered
	// ValueType, not the lowest).
	public static final ValueType ValueTypeForSeek = ValueType.Value;
	
	public static final long MaxSequenceNumber =((((long)1) << 56) - 1);
	

	// Returns the user key portion of an internal key.
	public static Slice extractUserKey(Slice internalKey) {
		return null;
	}
	
	public static ValueType extractValueType(Slice internalKey) {
		return null;
	}
}
