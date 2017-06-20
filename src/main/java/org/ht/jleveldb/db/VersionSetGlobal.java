package org.ht.jleveldb.db;

import java.util.ArrayList;

import org.ht.jleveldb.Iterator0;
import org.ht.jleveldb.Options;
import org.ht.jleveldb.ReadOptions;
import org.ht.jleveldb.Status;
import org.ht.jleveldb.db.format.DBFormat;
import org.ht.jleveldb.db.format.InternalKey;
import org.ht.jleveldb.db.format.InternalKeyComparator;
import org.ht.jleveldb.table.TwoLevelIterator;
import org.ht.jleveldb.util.Coding;
import org.ht.jleveldb.util.Comparator0;
import org.ht.jleveldb.util.Slice;

public class VersionSetGlobal {
	public static int targetFileSize(Options options) {
		return options.maxFileSize;
	}
	
	// Maximum bytes of overlaps in grandparent (i.e., level+2) before we
	// stop building a single file in a level->level+1 compaction.
	public static long maxGrandParentOverlapBytes(Options options) {
		return 10 * targetFileSize(options);
	}

	// Maximum number of bytes in all compacted files.  We avoid expanding
	// the lower level file set of a compaction if it would make the
	// total compaction cover more than this many bytes.
	public static long expandedCompactionByteSizeLimit(Options options) {
		return 25 * targetFileSize(options);
	}
	
	public static double maxBytesForLevel(Options options, int level) {
		// Note: the result for level zero is not really used since we set
		// the level-0 compaction threshold based on number of files.

		// Result for both level-0 and level-1
		double result = 10. * 1048576.0;
		while (level > 1) {
		    result *= 10;
		    level--;
		}
		return result;
	}
	
	public static long maxFileSizeForLevel(Options options, int level) {
		// We could vary per level to reduce number of files?
		return targetFileSize(options);
	}
	
	public static long totalFileSize(ArrayList<FileMetaData> files) {
		long sum = 0;
		for (int i = 0; i < files.size(); i++) {
			sum += files.get(i).fileSize;
		}
		return sum;
	}
	
	//TODO: may has bugs
	public static int findFile(InternalKeyComparator icmp,
			ArrayList<FileMetaData> files,
			Slice key) {
		int left = 0;
		int right = files.size();

		while (left < right) {
			int mid = (left + right) / 2;
			FileMetaData f = files.get(mid);
			if (icmp.compare(f.largest.encode(), key) < 0) {
				// Key at "mid.largest" is < "target".  Therefore all
				// files at or before "mid" are uninteresting.
				left = mid + 1;
			} else {
				// Key at "mid.largest" is >= "target".  Therefore all files
				// after "mid" are uninteresting.
				right = mid;
			}
		}
		return right;
	}
	
	static boolean afterFile(Comparator0 ucmp, Slice userKey, FileMetaData f) {
		// NULL user_key occurs before all keys and is therefore never after *f
		return (userKey != null && ucmp.compare(userKey, f.largest.userKey()) > 0);
	}
	
	static boolean beforeFile(Comparator0 ucmp, Slice userKey, FileMetaData f) {
		// NULL user_key occurs after all keys and is therefore never before *f
		return (userKey != null && ucmp.compare(userKey, f.smallest.userKey()) < 0);
	}
	
	public static boolean someFileOverlapsRange(
		    InternalKeyComparator icmp,
		    boolean disjointSortedFiles,
		    ArrayList<FileMetaData> files,
		    Slice smallestUserKey,
		    Slice largestUserKey) {
		  Comparator0 ucmp = icmp.userComparator();
		  if (!disjointSortedFiles) {
			  // Need to check against all files
			  for (int i = 0; i < files.size(); i++) {
				  FileMetaData f = files.get(i);
				  if (afterFile(ucmp, smallestUserKey, f) ||
		          beforeFile(ucmp, largestUserKey, f)) {
					  // No overlap
				  } else {
					  return true;  // Overlap
				  }
			  }
			  return false;
		  }

		  // Binary search over file list
		  int index = 0;
		  if (smallestUserKey != null) {
			  // Find the earliest possible internal key for smallest_user_key
			  InternalKey small = new InternalKey(smallestUserKey, DBFormat.kMaxSequenceNumber, DBFormat.kValueTypeForSeek);
			  index = findFile(icmp, files, small.encode());
		  }

		  if (index >= files.size()) {
			  // beginning of range is after all files, so no overlap.
			  return false;
		  }

		  return !beforeFile(ucmp, largestUserKey, files.get(index));
	}
	
	public static TwoLevelIterator.BlockFunction getFileIterator = new TwoLevelIterator.BlockFunction() {
		public Iterator0 run(Object arg, ReadOptions options, Slice fileValue) {
			TableCache cache = (TableCache)(arg);
			if (fileValue.size() != 16) {
				return Iterator0.newErrorIterator(Status.corruption("FileReader invoked with unexpected value"));
			} else {
				return cache.newIterator(options,
						Coding.decodeFixedNat64(fileValue.data(), fileValue.offset),
						Coding.decodeFixedNat64(fileValue.data(), fileValue.offset + 8));
			}
		}
	};
}
