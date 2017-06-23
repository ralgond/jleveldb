package org.ht.jleveldb.db.format;

import java.util.List;

import org.ht.jleveldb.FilterPolicy;
import org.ht.jleveldb.util.ByteBuf;
import org.ht.jleveldb.util.Slice;

/**
 * Filter policy wrapper that converts from internal keys to user keys
 * 
 * @author Teng Huang ht201509@163.com
 */
public class InternalFilterPolicy extends FilterPolicy {
	
	FilterPolicy userPolicy;
	
	public InternalFilterPolicy(FilterPolicy userPolicy) {
		this.userPolicy = userPolicy;
	}
	
	public String name() {
		return userPolicy.name();
	}
	
	public void createFilter(List<Slice> keys, ByteBuf dst) {
		// We rely on the fact that the code in table.cc does not mind us
		// adjusting keys[].
		int n = keys.size();
		for (int i = 0; i < n; i++) {
			keys.set(i, DBFormat.extractUserKey(keys.get(i)));
		    // TODO(sanjay): Suppress dups?
		}
		userPolicy.createFilter(keys, dst);
	}
	
	public boolean keyMayMatch(Slice key, Slice f) {
		return userPolicy.keyMayMatch(DBFormat.extractUserKey(key), f);
	}
}
