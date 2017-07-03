package com.tchaicatkovsky.jleveldb.benchmark;

public class DBBench {
	// Comma-separated list of operations to run in the specified order
	//  Actual benchmarks:
	//     fillseq       -- write N values in sequential key order in async mode
	//     fillrandom    -- write N values in random key order in async mode
	//     overwrite     -- overwrite N values in random key order in async mode
	//     fillsync      -- write N/100 values in random key order in sync mode
	//     fill100K      -- write N/1000 100K values in random order in async mode
	//     deleteseq     -- delete N keys in sequential order
	//     deleterandom  -- delete N keys in random order
	//     readseq       -- read N times sequentially
	//     readreverse   -- read N times in reverse order
	//     readrandom    -- read N times in random order
	//     readmissing   -- read N missing keys in random order
	//     readhot       -- read N times in random order from 1% section of DB
	//     seekrandom    -- N random seeks
	//     open          -- cost of opening a DB
	//     crc32c        -- repeated crc32c of 4K of data
	//     acquireload   -- load N*1000 times
	//  Meta operations:
	//     compact     -- Compact the entire DB
	//     stats       -- Print DB stats
	//     sstables    -- Print sstable info
	//     heapprofile -- Dump a heap profile (if supported by this port)
	
	
	
}
