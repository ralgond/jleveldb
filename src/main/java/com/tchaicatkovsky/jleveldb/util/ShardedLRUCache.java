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

//LRU cache implementation
//
//Cache entries have an "in_cache" boolean indicating whether the cache has a
//reference on the entry.  The only ways that this can become false without the
//entry being passed to its "deleter" are via Erase(), via Insert() when
//an element with a duplicate key is inserted, or on destruction of the cache.
//
//The cache keeps two linked lists of items in the cache.  All items in the
//cache are in one list or the other, and never both.  Items still referenced
//by clients but erased from the cache are in neither list.  The lists are:
//- in-use:  contains the items currently referenced by clients, in no
//particular order.  (This list is used for invariant checking.  If we
//removed the check, elements that would otherwise be on this list could be
//left as disconnected singleton lists.)
//- LRU:  contains the items not currently referenced by clients, in LRU order
//Elements are moved between these lists by the Ref() and Unref() methods,
//when they detect an element in the cache acquiring or losing its only
//external reference.

//An entry is a variable length heap-allocated structure.  Entries
//are kept in a circular doubly linked list ordered by access time.
public class ShardedLRUCache extends Cache {
	
	static class LRUHandle extends Cache.Handle{
		Object value;
		Deleter deleter;
		LRUHandle nextHash;
		LRUHandle next;
		LRUHandle prev;
		int charge;			//TODO: Should use long 
		int keyLength;
		boolean inCache;    // Whether entry is in the cache.
		int refs;      		// References, including cache reference, if present.
		long hash;      	// Hash of key(); used for fast sharding and comparisons
		Slice keyData;
		
		Slice key() {
			// For cheaper lookups, we allow a temporary Handle object
		    // to store a pointer to a key in "value".
		    if (next == this) {
		    	return (Slice)value;
		    } else {
		        return keyData;
		    }
		}
	}
	
	/**
	 * We provide our own simple hash table since it removes a whole bunch 
	 * of porting hacks and is also faster than some of the built-in hash 
	 * table implementations in some of the compiler/runtime combinations 
	 * we have tested.
	 */
	static class HandleTable {
		// The table consists of an array of buckets where each bucket is
		// a linked list of cache entries that hash into the bucket.
		long length;
		int elems;
		LRUHandle[] list = new LRUHandle[0];
		
		public LRUHandle lookup(Slice key, long hash) {
		    return findPointer(key, hash, null);
		}
		
		public HandleTable() {
			resize();
		}
		
		public LRUHandle insert(LRUHandle h) {
			Object0<LRUHandle> prev = new Object0<LRUHandle>();
		    LRUHandle old = findPointer(h.key(), h.hash, prev);
		    h.nextHash = (old == null ? null : old.nextHash);
		    prev.getValue().nextHash = h;
		    if (old == null) {
		    	++elems;
		    	if (elems > length) {
		    		// Since each cache entry is fairly large, we aim for a small
		    		// average linked list length (<= 1).
		    		resize();
		    	}
		    }
		    return old;
		}
		
		public LRUHandle remove(Slice key, long hash) {
			Object0<LRUHandle> prev = new Object0<LRUHandle>();
		    LRUHandle h = findPointer(key, hash, prev);
		    if (h != null) {
		    	prev.getValue().nextHash = h.nextHash;
		        --elems;
		    }
		    return h;
		}
		
		/**
		 * Return a pointer to slot that points to a cache entry that
		 * matches key/hash.  If there is no such cache entry, return a
		 * pointer to the trailing slot in the corresponding linked list.
		 * @param key
		 * @param hash
		 * @param prev
		 * @return
		 */
		LRUHandle findPointer(Slice key, long hash, Object0<LRUHandle> prev) {
			LRUHandle ptr = list[(int)(hash % length)];
		    assert(ptr != null);
		    while (ptr.nextHash != null) {
		        if (ptr.nextHash.hash == hash && key.equals(ptr.nextHash.key())) {
		        	break;
		        }
		        ptr = ptr.nextHash;
		    }
		    if (prev != null)
		    	prev.setValue(ptr);
		    return ptr.nextHash;
		}
		
		void resize() {
		    int newLength = 4;
		    while (newLength < elems) {
		    	newLength *= 2;
		    }
		    LRUHandle[] newList = new LRUHandle[newLength];
		    for (int i = 0; i < newList.length; i++)
		    	newList[i] = new LRUHandle();
		    
		    int count = 0;
		    for (int i = 0; i < length; i++) {
		    	LRUHandle h = list[i].nextHash;
		    	while (h != null) {
		    		LRUHandle next = h.nextHash;
		    		long hash = h.hash;
		    		LRUHandle ptr = newList[(int)(hash % newLength)];
		    		h.nextHash = ptr.nextHash;
		    		ptr.nextHash = h;
		    		h = next;
		    		count++;
		    	}
		    }
		    assert(elems == count);
		    list = null;
		    list = newList;
		    length = newLength;
		}
	}
	
	/** 
	 * A single shard of sharded cache.
	 */
	class SingleShardLRUCache {
		/** 
		 * Initialized before use.
		 */
		int capacity;

		/**
		 * mutex protects the following state.
		 */
		Mutex mutex = new Mutex();
		int usage;

		/**
		 * Dummy head of LRU list.
		 * lru.prev is newest entry, lru.next is oldest entry.
		 * Entries have refs==1 and in_cache==true.
		 */
		LRUHandle lru = new LRUHandle();

		/**
		 * Dummy head of in-use list.
		 * Entries are in use by clients, and have refs >= 2 and inCache==true.
		 */
		LRUHandle inUse = new LRUHandle();

		HandleTable table = new HandleTable();
		
		public SingleShardLRUCache() {
			usage = 0;
			// Make empty circular linked lists.
			lru.next = lru;
			lru.prev = lru;
			inUse.next = inUse;
			inUse.prev = inUse;
		}
		
		int lruListSize() {
			int ret = 0;
			LRUHandle head = lru.next;
			while (head != lru) {
				ret += 1;
				head = head.next;
			}
			return ret;
		}
		
		int inUseListSize() {
			int ret = 0;
			LRUHandle head = inUse.next;
			while (head != inUse) {
				ret += 1;
				head = head.next;
			}
			return ret;
		}
		
		void delete() {
			assert(inUse.next == inUse);  // Error if caller has an unreleased handle
			for (LRUHandle e = lru.next; e != lru; ) {
			    LRUHandle next = e.next;
			    assert(e.inCache);
			    e.inCache = false;
			    assert(e.refs == 1);  // Invariant of lru list.
			    unref(e);
			    e = next;
			}
		}
		
		/** 
		 * Separate from constructor so caller can easily make an array of LRUCache
		 * @param capacity
		 */
		void setCapacity(int capacity) { 
			this.capacity = capacity; 
		}
		
		/** 
		 * Like Cache methods, but with an extra "hash" parameter.
		 * @param key
		 * @param hash
		 * @param value
		 * @param charge
		 * @param deleter
		 * @return
		 */
		Cache.Handle insert(Slice key, long hash, Object value, int charge, Deleter deleter) {
			mutex.lock();
			try {
				LRUHandle e = new LRUHandle();
				e.value = value;
				e.deleter = deleter;
				e.charge = charge;
				e.keyLength = key.size();
				e.hash = hash;
				e.inCache = false;
				e.refs = 1;
				e.keyData = key;
				
				if (capacity > 0) {
					e.refs++;  // for the cache's reference.
					e.inCache = true;
					LRU_Append(inUse, e);
					usage += charge;
					finishErase(table.insert(e));
				} // else don't cache.  (Tests use capacity_==0 to turn off caching.)
//				else {
//					deleter.run(key, value);
//				}
				
				while (usage > capacity && lru.next != lru) {
				    LRUHandle old = lru.next;
				    assert(old.refs == 1);
				    boolean erased = finishErase(table.remove(old.key(), old.hash));
				    if (!erased) {  // to avoid unused variable when compiled NDEBUG
				    	assert(erased);
				    }
				}
				return (Cache.Handle)e;
			} finally {
				mutex.unlock();
			}
		}
		
		public Cache.Handle lookup(Slice key, long hash) {
			mutex.lock();
			try {
				LRUHandle e = table.lookup(key, hash);
				if (e != null) {
				    ref(e);
				}
				return (Cache.Handle)e;
			} finally {
				mutex.unlock();
			}
		}

		public void release(Cache.Handle handle) {
			mutex.lock();
			try {
				unref((LRUHandle)handle);

			} finally {
				mutex.unlock();
			}
		}
		
		public void erase(Slice key, long hash) {
			mutex.lock();
			try {
				finishErase(table.remove(key, hash));
			} finally {
				mutex.unlock();
			}
		}
		
		public void prune() {
			mutex.lock();
			try {
				while (lru.next != lru) {
					LRUHandle e = lru.next;
					assert(e.refs == 1);
					boolean erased = finishErase(table.remove(e.key(), e.hash));
					if (!erased) {  // to avoid unused variable when compiled NDEBUG
						assert(erased);
					}
				}
			} finally {
				mutex.unlock();
			}
		}
		
		int totalCharge() {
		    mutex.lock();
		    try {
		    	return usage;
		    } finally {
		    	mutex.unlock();
		    }
		}

		void LRU_Remove(LRUHandle e) {
			e.next.prev = e.prev;
			e.prev.next = e.next;			
		}
		  
		void LRU_Append(LRUHandle list, LRUHandle e) {			
			// Make "e" newest entry by inserting just before *list
			e.next = list;
			e.prev = list.prev;
			e.prev.next = e;
			e.next.prev = e;
		}
		
		void ref(LRUHandle e) {
			if (e.refs == 1 && e.inCache) {  // If on lru list, move to inUse list.
			    LRU_Remove(e);
			    LRU_Append(inUse, e);
			}
			e.refs++;
		}
		  
		void unref(LRUHandle e) {
			assert(e.refs > 0);
			e.refs--;
			if (e.refs == 0) { // Deallocate.
				assert(!e.inCache);
			    e.deleter.run(e.key(), e.value);
			    e = null; // delete e;
			} else if (e.inCache && e.refs == 1) {  // No longer in use; move to lru list.
			    LRU_Remove(e);
			    LRU_Append(lru, e);
			}
		}
		  
		boolean finishErase(LRUHandle e) {
			if (e != null) {
				assert(e.inCache);
				LRU_Remove(e);
				e.inCache = false;
				usage -= e.charge;
				unref(e);
			}
			return e != null;
		}
		
		String dumpHandleList(int slot) {
			return String.format("Cache.HandleList, slot=%d, lru.size=%d, inUse.size=%d\n", slot, lruListSize(), inUseListSize());
		}
	}
		
	final static int kNumShardBits = 4;
	final static int kNumShards = 1 << kNumShardBits;
	
	
	SingleShardLRUCache[] shard;
	Mutex idMutex;
	long lastId;
	public ShardedLRUCache(int capacity) {
		shard = new SingleShardLRUCache[kNumShards];
		idMutex = new Mutex();
		final int perShard = (capacity + (kNumShards - 1)) / kNumShards;
		for (int i = 0; i < shard.length; i++) {
			shard[i] = new SingleShardLRUCache();
			shard[i].setCapacity(perShard);
		}
	}
	
	public void delete() {
		for (int i = 0; i < shard.length; i++) {
			shard[i].delete();
		}
	}
	
	static int calcShard(long hash) {
	    return (int)(hash >> (32 - kNumShardBits));
    }
	
	@Override
	public Handle insert(Slice key, Object value, int charge, Deleter deleter) {
		long hash = key.hashCode0();
		if (hash < 0)
			hash *= -1;
		
		return shard[calcShard(hash)].insert(key, hash, value, charge, deleter);
	}

	@Override
	public Handle lookup(Slice key) {
	    long hash = key.hashCode0();
	    if (hash < 0)
			hash *= -1;
	    
	    return shard[calcShard(hash)].lookup(key, hash);
	}

	@Override
	public void release(Handle handle) {
	    LRUHandle h = (LRUHandle)handle;
	    
	    shard[calcShard(h.hash)].release(handle);
	}

	@Override
	public Object value(Handle handle) {
		return ((LRUHandle)handle).value;
	}

	@Override
	public void erase(Slice key) {
	    long hash = key.hashCode0();
	    if (hash < 0)
			hash *= -1;
	    
	    shard[calcShard(hash)].erase(key, hash);
	}

	@Override
	public long newId() {
	    idMutex.lock();
	    try {
	    	 return ++(lastId);
	    } finally {
	    	idMutex.unlock();
	    }
	}

	@Override
	public long totalCharge() {
		long total = 0;
	    for (int s = 0; s < kNumShards; s++) {
	      total += shard[s].totalCharge();
	    }
	    return total;
	}
	
	@Override
	public void prune() {
		for (int s = 0; s < kNumShards; s++) {
			shard[s].prune();
		}
	}
	
	@Override
	public void debugPrint() {
		for (int i = 0; i < shard.length; i++) {
			System.out.printf("[DEBUG] Cache debug, handleList=%s", shard[i].dumpHandleList(i));
		}
	}
}

