/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tchaicatkovsky.jleveldb.benchmark;

//TODO
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
