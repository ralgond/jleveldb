package com.tchaicatkovsky.jleveldb;

public class SnapshotList {
	
	public SnapshotList() {
		list.next = list;
		list.prev = list;
	}
	
	public boolean isEmpty() {
		return list.next == list;
	}
	
	public Snapshot oldest() {
		assert(!isEmpty());
		return list.next;
	}
	
	public Snapshot newest() {
		assert(!isEmpty());
		return list.prev;
	}
	
	public Snapshot new0(long seq) {
		Snapshot s = new Snapshot(seq, this);
		s.next = list;
		s.prev = list.prev;
		s.prev.next = s;
		s.next.prev = s;
		return s;
	}
	
	public void delete(Snapshot s) {
		assert(s.list == this);
		s.prev.next = s.next;
		s.next.prev=  s.prev;
	}
	
	Snapshot list = new Snapshot();
}
