package com.tchaicatkovsky.jleveldb;

import com.tchaicatkovsky.jleveldb.util.DefaultSlice;
import com.tchaicatkovsky.jleveldb.util.Slice;

public class EmptyIterator0 extends Iterator0 {

	Status status;
	
	public EmptyIterator0(Status s) {
		status = s;
	}
	
	@Override
	public void delete() {
		super.delete();
	}

	@Override
	public boolean valid() {
		return false;
	}

	@Override
	public void seekToFirst() {

	}

	@Override
	public void seekToLast() {

	}

	@Override
	public void seek(Slice target) {

	}

	@Override
	public void next() {
		assert(false);
	}

	@Override
	public void prev() {
		assert(false);
	}

	@Override
	public Slice key() {
		assert(false);
		return new DefaultSlice();
	}

	@Override
	public Slice value() {
		assert(false);
		return new DefaultSlice();
	}

	@Override
	public Status status() {
		return status;
	}

}
