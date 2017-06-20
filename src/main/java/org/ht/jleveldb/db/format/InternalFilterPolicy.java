package org.ht.jleveldb.db.format;

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
		return null;
	}
	
	public void createFilter(Slice[] keys, int n, ByteBuf buf) {
		
	}
	
	public boolean keyMayMatch(Slice key, Slice filter) {
		return false;
	}
}
