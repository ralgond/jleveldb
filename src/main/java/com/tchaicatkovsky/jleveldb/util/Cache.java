/**
 * Copyright (c) 2017-2018, Teng Huang <ht201509 at 163 dot com>
 * All rights reserved.
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tchaicatkovsky.jleveldb.util;

public abstract class Cache {
	public static class Handle {
		
	}
	
	public interface Deleter {
		void run(Slice key, Object value);
	}
	
	public abstract void delete();

	/**
	 * Insert a mapping from key->value into the cache and assign it
	 * the specified charge against the total cache capacity.</br></br>
	 * 
	 * Returns a handle that corresponds to the mapping.  The caller
	 * must call this->Release(handle) when the returned mapping is no
	 * longer needed.</br></br>
	 * 
	 * When the inserted entry is no longer needed, the key and 
	 * value will be passed to "deleter".
	 * @param key
	 * @param value
	 * @param charge
	 * @param deleter
	 * @return
	 */
	public abstract Handle insert(Slice key, Object value, int charge, Deleter deleter);
	
	/**
	 * If the cache has no mapping for "key", returns null.</br></br>
	 * 
	 * Else return a handle that corresponds to the mapping.  The caller
	 * must call this.release(handle) when the returned mapping is no 
	 * longer needed.
	 * 
	 * @param key
	 * @return
	 */
	public abstract Handle lookup(Slice key);
	
	/**
	 * Release a mapping returned by a previous lookup().</br>
	 * REQUIRES: handle must not have been released yet.</br>
	 * REQUIRES: handle must have been returned by a method on this.</br>
	 * @param handle
	 */
	public abstract void release(Handle handle);
	
	/**
	 * Return the value encapsulated in a handle returned by a
	 * successful Lookup().</br>
	 * REQUIRES: handle must not have been released yet.</br>
	 * REQUIRES: handle must have been returned by a method on this.</br>
	 * @param handle
	 * @return
	 */
	public abstract Object value(Handle handle);

	/**
	 * If the cache contains entry for key, erase it.  Note that the 
	 * underlying entry will be kept around until all existing handles 
	 * to it have been released.
	 * 
	 * @param key
	 */
	public abstract void erase(Slice key);
	
	/**
	 * Return a new numeric id.  May be used by multiple clients who are 
	 * sharing the same cache to partition the key space.  Typically the 
	 * client will allocate a new id at startup and prepend the id to 
	 * its cache keys.
	 * @return
	 */
	public abstract long newId();
	
	/**
	 * Remove all cache entries that are not actively in use.  Memory-constrained 
	 * applications may wish to call this method to reduce memory usage. 
	 *  Default implementation of prune() does nothing.  Subclasses are strongly 
	 *  encouraged to override the default implementation.  A future release of 
	 *  jleveldb may change prune() to a pure abstract method.
	 */
	public void prune() {
		
	}

	/**
	 * Return an estimate of the combined charges of all elements stored in the
	 * cache.
	 * 
	 * @return
	 */
	public abstract long totalCharge();

	/**
	 * Create a new cache with a fixed size capacity.  This implementation 
	 * of Cache uses a least-recently-used eviction policy.
	 * 
	 * @param capacity
	 * @return
	 */
	public static Cache newLRUCache(int capacity) {
		return new ShardedLRUCache(capacity);
	}
	
	public abstract void debugPrint();
}
