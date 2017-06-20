package org.ht.jleveldb;

import org.ht.jleveldb.util.Slice;

public abstract class Iterator0 {
	public abstract void delete();
	
	public abstract boolean valid();
	
	public abstract void seekToFirst();
	
	public abstract void seekToLast();
	
	public abstract void seek(Slice target);
	
	public abstract void next();
	
	public abstract void prev();
	
	public abstract Slice key();
	
	public abstract Slice value();
	
	public abstract Status status();
	
	public void registerCleanup(Runnable runnable) {
		//TODO
		Cleanup c = new Cleanup();
		c.runnable = runnable;
		
		c.next = cleanup;
		cleanup = c;
	}
	
	static class Cleanup {
		Runnable runnable;
		Cleanup next;
	}
	
	Cleanup cleanup = new Cleanup();
	
	public static Iterator0 newEmptyIterator() {
		//TODO
		return null;
	}
	
	public static Iterator0 newErrorIterator(Status status) {
		//TODO
		return null;
	}
}
