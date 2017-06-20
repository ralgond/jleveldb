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
		//TODO
	}
	
	public void startBlock(long blockOffset) {
		long filter_index = (blockOffset / kFilterBase);
		assert(filter_index >= filterOffsets.size());
		while (filter_index > filterOffsets.size()) {
		    generateFilter();
		}
	}
	
	public void addKey(Slice key) {
		Slice k = key.clone();
		start.add(keys.size());
		keys.append(k.data(), k.offset, k.size());
	}
	
	public Slice finish() {
		if (!start.isEmpty()) {
		    generateFilter();
		}

		// Append array of per-filter offsets
		int array_offset = result.size();
		for (int i = 0; i < filterOffsets.size(); i++) {
		    result.writeFixedNat32(filterOffsets.get(i));
		}

		result.writeFixedNat32(array_offset);
		result.addByte((byte)(kFilterBaseLg & 0xff));  // Save encoding parameter in result
		return new Slice(result);
	}
	
	
	public void generateFilter() {
		int num_keys = start.size();
		if (num_keys == 0) {
		    // Fast path if there are no keys for this filter
		    filterOffsets.add(result.size());
		    return;
		}

		// Make list of keys from flattened key structure
		start.add(keys.size());  // Simplify length computation
		tmpKeys.clear();
		for (int i = 0; i < num_keys; i++) {
		    byte[] base = keys.data();
		    int baseOffset = start.get(i);
		    int length = start.get(i+1) - start.get(i);
		    tmpKeys.add(new Slice(base, baseOffset, length));  //TODO may has bugs, be careful
		}

		  // Generate filter for current set of keys and append to result_.
		filterOffsets.add(result.size());
		policy.createFilter((Slice[])tmpKeys.toArray(), num_keys, result);

		tmpKeys.clear();
		keys.clear();
		start.clear();
	}
	
	
}
