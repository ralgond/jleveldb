package org.ht.jleveldb.db;

public class Snapshot {
	public long number; // const after creation
	
	public Snapshot prev;
	public Snapshot next;
	
	public SnapshotList list;
	
	public Snapshot(long number, SnapshotList list) {
		this.number = number;
		this.list = list;
	}
}
