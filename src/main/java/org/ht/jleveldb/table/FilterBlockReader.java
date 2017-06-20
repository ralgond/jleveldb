package org.ht.jleveldb.table;

import org.ht.jleveldb.FilterPolicy;
import org.ht.jleveldb.util.Coding;
import org.ht.jleveldb.util.Slice;

public class FilterBlockReader {
	public void delete() {
		//TODO
	}
	
	// REQUIRES: "contents" and *policy must stay live while *this is live.
	public FilterBlockReader(FilterPolicy policy, Slice contents) {
		this.policy = policy;
		this.data = null;
		begin = 0;
		end = 0;
		num = 0;
		baseLg = 0;
		
		int n = contents.size();
		if (n < 5) return;  // 1 byte for base_lg_ and 4 for start of offset array //TODO: add exception
		baseLg = contents.data[contents.offset + n - 1];
		int lastWord = Coding.decodeFixedNat32(contents.data, contents.offset+n-5);
		if (lastWord > n - 5) return; //TODO: add exception
		data = contents.data;
		begin  = contents.offset;
		end  = contents.offset + lastWord;
		num = (n - 5 - lastWord) / 4;
	}
	
	public boolean keyMayMatch(long blockOffset, Slice key) {
		long index = blockOffset >> baseLg;
		if (index < num) {
			int start = Coding.decodeFixedNat32(data, (int)(end + index * 4));
		    int limit = Coding.decodeFixedNat32(data, (int)(end + index*4 + 4));
		    if (start <= limit && limit <= (end - begin)) {
		    	Slice filter = new Slice(data, begin + start, limit - start);
		    	return policy.keyMayMatch(key, filter);
		    } else if (start == limit) {
		      // Empty filters do not match any keys
		      return false;
		    }
		}
		return true;  // Errors are treated as potential matches
	}

	final FilterPolicy policy;
	byte[] data;    
	int begin;	// Pointer to filter data (at block-start)
	int end;	// Pointer to filter data (at block-end)
	int num;          // Number of entries in offset array
	int baseLg;      // Encoding parameter (see kFilterBaseLg in .cc file)
}
