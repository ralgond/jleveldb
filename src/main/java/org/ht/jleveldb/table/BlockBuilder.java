package org.ht.jleveldb.table;

import java.util.ArrayList;

import org.ht.jleveldb.Options;
import org.ht.jleveldb.util.ByteBuf;
import org.ht.jleveldb.util.Slice;

public class BlockBuilder {
	public BlockBuilder(Options options) {
		assert(options.blockRestartInterval >= 1);
		this.options = options.cloneOptions();
		counter = 0;
		finished = false;
		restarts = new ArrayList<Integer>();
		restarts.add(0);
	}
	
	public void reset() {
		buffer.clear();
		restarts.clear();
		restarts.add(0);
		counter = 0;
		finished = false;
		lastKey.clear();
	}
	
	public void add(Slice key, Slice value) throws Exception {
		Slice lastKeyPiece = new Slice(lastKey);
		assert(!finished);
		assert(counter <= options.blockRestartInterval);
		assert(buffer.empty() // No values yet?
		         || options.comparator.compare(key, lastKeyPiece) > 0);
		
		int shared = 0;
		if (counter < options.blockRestartInterval) {
			// See how much sharing to do with previous string
		    int minLength = Integer.min(lastKeyPiece.size(), key.size());
		    while ((shared < minLength) && (lastKeyPiece.getByte(shared) == key.getByte(shared))) {
		      shared++;
		    }
		} else {
		    // Restart compression
		    restarts.add(buffer.size());
		    counter = 0;
		}
		int nonShared = key.size() - shared;
		
		// Add "<shared><non_shared><value_size>" to buffer_
		buffer.writeVarNat32(shared); 
		buffer.writeVarNat32(nonShared);
		buffer.writeVarNat32(value.size());
		
		// Add string delta to buffer_ followed by value
		buffer.append(key.data, key.offset + shared, nonShared);
		buffer.append(value.data, value.offset, value.size());
		
		lastKey.resize(shared);
		lastKey.append(key.data,  key.offset + shared, nonShared);
		
		//TODO://assert(Slice(last_key_) == key);
		counter++;
	}
	
	public Slice finish() throws Exception {
		// Append restart array
		for (int i = 0; i < restarts.size(); i++) {
		    buffer.writeFixedNat32(restarts.get(i));
		}
		buffer.writeFixedNat32(restarts.size());
		
		finished = true;
		return new Slice(buffer);
	}
	
	public int currentSizeEstimate() {
		return (buffer.size() +         // Raw data buffer
		        restarts.size() * 4 +   // Restart array
		        4);                     // Restart array length
	}
	
	public boolean empty() {
		return buffer.size() == 0;
	}
	
	Options options;
	ByteBuf buffer; // Destination buffer
	ArrayList<Integer> restarts; // Restart points
	int counter; // Number of entries emitted since restart
	boolean finished; // Has Finish() been called?
	ByteBuf lastKey;
}
