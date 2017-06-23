package org.ht.jleveldb;

import org.ht.jleveldb.util.ByteBuf;
import org.ht.jleveldb.util.Slice;

public interface DB {
	
	Status Open(Options options, String name) throws Exception;
	
	Status put(WriteOptions options, Slice key, Slice value) throws Exception;
	
	Status delete(WriteOptions options, Slice key) throws Exception;
	
	Status write(WriteOptions options, WriteBatch updates) throws Exception;
	
	/**
	 * 
	 * @param options
	 * @param key
	 * @param value [OUTPUT]
	 * @return
	 * @throws Exception
	 */
	Status get(ReadOptions options,  Slice key, ByteBuf value) throws Exception;
}
