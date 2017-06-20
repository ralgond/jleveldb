package org.ht.jleveldb.util;

public abstract class Cache {
	public static class Handle {
		
	}
	
	public interface Deleter {
		void run(Slice key, Object value);
	}
	
	  // Insert a mapping from key->value into the cache and assign it
	  // the specified charge against the total cache capacity.
	  //
	  // Returns a handle that corresponds to the mapping.  The caller
	  // must call this->Release(handle) when the returned mapping is no
	  // longer needed.
	  //
	  // When the inserted entry is no longer needed, the key and
	  // value will be passed to "deleter".
	public abstract Handle insert(Slice key, Object value, int charge, Deleter deleter);
	
	  // If the cache has no mapping for "key", returns NULL.
	  //
	  // Else return a handle that corresponds to the mapping.  The caller
	  // must call this->Release(handle) when the returned mapping is no
	  // longer needed.
	public abstract Handle lookup(Slice key);
	
	  // Release a mapping returned by a previous Lookup().
	  // REQUIRES: handle must not have been released yet.
	  // REQUIRES: handle must have been returned by a method on *this.
	public abstract void release(Handle handle);
	
	
	  // Return the value encapsulated in a handle returned by a
	  // successful Lookup().
	  // REQUIRES: handle must not have been released yet.
	  // REQUIRES: handle must have been returned by a method on *this.
	public abstract Object value(Handle handle);
	
	  // If the cache contains entry for key, erase it.  Note that the
	  // underlying entry will be kept around until all existing handles
	  // to it have been released.
	public abstract void erase(Slice key);
	
	  // Return a new numeric id.  May be used by multiple clients who are
	  // sharing the same cache to partition the key space.  Typically the
	  // client will allocate a new id at startup and prepend the id to
	  // its cache keys.
	public abstract long newId();
	
	  // Remove all cache entries that are not actively in use.  Memory-constrained
	  // applications may wish to call this method to reduce memory usage.
	  // Default implementation of Prune() does nothing.  Subclasses are strongly
	  // encouraged to override the default implementation.  A future release of
	  // leveldb may change Prune() to a pure abstract method.
	public void prune() {
		
	}
	
	  // Return an estimate of the combined charges of all elements stored in the
	  // cache.
	public abstract int totalCharge();
	
	
	// Create a new cache with a fixed size capacity.  This implementation
	// of Cache uses a least-recently-used eviction policy.
	public static Cache newLRUCache(int capacity) {
		return new ShardedLRUCache(capacity);
	}
}
