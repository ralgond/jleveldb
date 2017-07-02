package com.tchaicatkovsky.jleveldb;

public class ReadOptions {
	/**
	 * If true, all data read from underlying storage will be
	 * verified against corresponding checksums.
	 * Default: false
	 */
	public boolean verifyChecksums;
	
	/**
	 * Should the data read for this iteration be cached in memory?
	 * Callers may wish to set this field to false for bulk scans.
	 * Default: true
	 */
	public boolean fillCache;
	
	/**
	 * If "snapshot" is non-null, read as of the supplied snapshot
	 * (which must belong to the DB that is being read and which must
	 * not have been released).  If "snapshot" is null, use an implicit
	 * snapshot of the state at the beginning of this read operation.
	 * Default: null
	 */
	public Snapshot snapshot;
	
	public ReadOptions() {
		verifyChecksums = false;
		fillCache = true;
		snapshot = null;
	}
	
	@Override
	public ReadOptions clone() {
		ReadOptions ro = new ReadOptions();
		ro.verifyChecksums = verifyChecksums;
		ro.fillCache = fillCache;
		ro.snapshot = snapshot;
		return ro;
	}
}
