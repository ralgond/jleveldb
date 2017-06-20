package org.ht.jleveldb;

import org.ht.jleveldb.util.ByteBuf;
import org.ht.jleveldb.util.Slice;

public interface DB {
	
	Status Open(Options options, String name);
	
	Status put(WriteOptions options, Slice key, Slice value);
	
	Status delete(WriteOptions options, Slice key);
	
	Status write(WriteOptions options, WriteBatch updates);
	
	/**
	 * 
	 * @param options
	 * @param key
	 * @param value [OUTPUT]
	 * @return
	 * @throws Exception
	 */
	Status get(ReadOptions options,  Slice key, ByteBuf value);
}
