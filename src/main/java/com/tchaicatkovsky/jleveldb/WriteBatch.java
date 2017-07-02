package com.tchaicatkovsky.jleveldb;

import com.tchaicatkovsky.jleveldb.db.WriteBatchInternal;
import com.tchaicatkovsky.jleveldb.db.format.ValueType;
import com.tchaicatkovsky.jleveldb.util.ByteBuf;
import com.tchaicatkovsky.jleveldb.util.ByteBufFactory;
import com.tchaicatkovsky.jleveldb.util.Coding;
import com.tchaicatkovsky.jleveldb.util.DefaultSlice;
import com.tchaicatkovsky.jleveldb.util.Slice;

public class WriteBatch {

	public WriteBatch() {
		clear();
	}
	/**
	 * Store the mapping "key->value" in the database.
	 * @param key
	 * @param value
	 */
	public void put(Slice key, Slice value) {
		WriteBatchInternal.setCount(this, WriteBatchInternal.count(this) + 1);
		rep.addByte(ValueType.Value.type());
		rep.writeLengthPrefixedSlice(key);
		rep.writeLengthPrefixedSlice(value);
	}
	
	/**
	 * If the database contains a mapping for "key", erase it.  Else do nothing.
	 * @param key
	 */
	public void delete(Slice key) {
		WriteBatchInternal.setCount(this, WriteBatchInternal.count(this) + 1);
		rep.addByte(ValueType.Deletion.type());
		rep.writeLengthPrefixedSlice(key);
	}
	
	/**
	 * Clear all updates buffered in this batch.
	 */
	public void clear() {
		rep.clear();
		rep.resize(WriteBatchInternal.kHEADER);
	}
	
	/**
	 * Support for iterating over the contents of a batch.
	 * @author thuang
	 *
	 */
	public interface Handler {
		void put(Slice key, Slice value);
		void delete(Slice key);
	}
	
	public Status iterate(Handler handler) {
		Slice input = new DefaultSlice(rep);
		if (input.size() < WriteBatchInternal.kHEADER) {
			return Status.corruption("malformed WriteBatch (too small)");
		}
		
		input.removePrefix(WriteBatchInternal.kHEADER);
		
		
		Slice key = new DefaultSlice();
		Slice value = new DefaultSlice();
		int found = 0;
		while (!input.empty()) {
			found++;
			byte tag = input.getByte(0);
			input.removePrefix(1);
			
			key.clear();
			value.clear();
			
			if (tag == ValueType.Value.type()) {
				if (Coding.popLengthPrefixedSlice(input, key) &&
						Coding.popLengthPrefixedSlice(input, value)) {
					handler.put(key, value);
				} else {
					return Status.corruption("bad WriteBatch Put");
				}
			} else if (tag == ValueType.Deletion.type()) {
				 if (Coding.popLengthPrefixedSlice(input, key)) {
					 handler.delete(key);
				 } else {
			          return Status.corruption("bad WriteBatch Delete");
			     }
			} else {
				return Status.corruption("unknown WriteBatch tag");
			}
		}
		if (found != WriteBatchInternal.count(this)) {
		    return Status.corruption("WriteBatch has wrong count");
		} else {
		    return Status.ok0();
		}
	}

	public ByteBuf rep = ByteBufFactory.defaultByteBuf();
}
