package org.ht.jleveldb.table;

import java.util.ArrayList;

import org.ht.jleveldb.FilterPolicy;
import org.ht.jleveldb.util.ByteBuf;
import org.ht.jleveldb.util.ByteBufFactory;
import org.ht.jleveldb.util.Slice;

public class FilterBlockBuilder {
	final public static int kFilterBaseLg = 11;
	final public static int kFilterBase = 1 << kFilterBaseLg;

	FilterPolicy policy;
	ByteBuf keys = ByteBufFactory.defaultByteBuf();     // Flattened key contents
	ArrayList<Integer> start = new ArrayList<>();     	// Starting index in keys_ of each key
	ByteBuf result = ByteBufFactory.defaultByteBuf();   // Filter data computed so far
	ArrayList<Slice> tmpKeys = new ArrayList<>();   	// policy_->CreateFilter() argument
	ArrayList<Integer> filterOffsets = new ArrayList<>();
	  
	public FilterBlockBuilder(FilterPolicy policy) {
		this.policy = policy;
	}
	
	public void delete() {
		
	}
	
	public void startBlock(long blockOffset) {
		long filterIndex = (blockOffset / kFilterBase);
		assert(filterIndex >= filterOffsets.size());
		while (filterIndex > filterOffsets.size()) {
		    generateFilter();
		}
	}
	
	public void addKey(Slice key) {
		start.add(keys.size());
		keys.append(key.data(), key.offset, key.size());
	}
	
	public Slice finish() {
		if (!start.isEmpty()) {
		    generateFilter();
		}

		// Append array of per-filter offsets
		int arrayOffset = result.size();
		for (int i = 0; i < filterOffsets.size(); i++)
		    result.writeFixedNat32(filterOffsets.get(i));

		result.writeFixedNat32(arrayOffset);
		
		result.addByte((byte)(kFilterBaseLg & 0xff));  // Save encoding parameter in result
		
		return new Slice(result);
	}
	
	
	public void generateFilter() {
		int numKeys = start.size();
		if (numKeys == 0) {
		    // Fast path if there are no keys for this filter
		    filterOffsets.add(result.size());
		    return;
		}

		// Make list of keys from flattened key structure
		start.add(keys.size());  // Simplify length computation
		tmpKeys.clear();
		for (int i = 0; i < numKeys; i++) {
		    byte[] base = keys.data();
		    int baseOffset = start.get(i);
		    int length = start.get(i+1) - start.get(i); // bytes
		    tmpKeys.add(new Slice(base, baseOffset, length));
		}

		  // Generate filter for current set of keys and append to result_.
		filterOffsets.add(result.size());
		policy.createFilter(tmpKeys, result);

		tmpKeys.clear();
		keys.clear();
		start.clear();
	}
}
