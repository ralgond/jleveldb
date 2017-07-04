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
package com.tchaicatkovsky.jleveldb.db;

import com.tchaicatkovsky.jleveldb.Env;
import com.tchaicatkovsky.jleveldb.FileName;
import com.tchaicatkovsky.jleveldb.Iterator0;
import com.tchaicatkovsky.jleveldb.Options;
import com.tchaicatkovsky.jleveldb.ReadOptions;
import com.tchaicatkovsky.jleveldb.Status;
import com.tchaicatkovsky.jleveldb.WritableFile;
import com.tchaicatkovsky.jleveldb.table.TableBuilder;
import com.tchaicatkovsky.jleveldb.util.Object0;
import com.tchaicatkovsky.jleveldb.util.Slice;

public class Builder {
	
	/**
	 * Build a Table file from the contents of iter.  The generated file 
	 * will be named according to meta.number.  On success, the rest of 
	 * meta will be filled with metadata about the generated table.
	 * If no data is present in iter, meta.fileSize will be set to
	 * zero, and no Table file will be produced.
	 * 
	 * @param dbname
	 * @param env
	 * @param options
	 * @param tcache
	 * @param iter
	 * @param meta
	 * @return
	 */
	public static Status buildTable(String dbname, Env env, Options options, 
			TableCache tcache, Iterator0 iter, FileMetaData meta) {
		
		
		Status s = Status.ok0();
		meta.fileSize = 0;
		iter.seekToFirst();
		
		String fname = FileName.getTableFileName(dbname, meta.number);
		if (iter.valid()) {
			Object0<WritableFile> fileFuncOut = new Object0<>();
			s = env.newWritableFile(fname, fileFuncOut);
			if (!s.ok())
				return s;
			
		
			WritableFile file = fileFuncOut.getValue();
			TableBuilder builder = new TableBuilder(options, file);
			meta.smallest.decodeFrom(iter.key());
			for (; iter.valid(); iter.next()) {
				Slice key = iter.key();
				meta.largest.decodeFrom(key);
				builder.add(key, iter.value());
				meta.numEntries++;
			}
			
			// Finish and check for builder errors
			if (s.ok()) {
				s = builder.finish();
				if (s.ok()) {
					meta.fileSize = builder.fileSize();
					assert(meta.fileSize > 0);
				}
			} else {
				builder.abandon();
			}
			
			
			// Finish and check for file errors
			if (s.ok()) {
				s = file.sync();
			}
			if (s.ok()) {
				s = file.close();
			}
			
			file.delete();
			file = null;
			builder.delete();
			builder = null;
			
			if (s.ok()) {
				// Verify that the table is usable
				Iterator0 it = tcache.newIterator(new ReadOptions(), meta.number, meta.fileSize);
				s = it.status();
				it.delete();
				it = null;
			}
			
		}
		
		// Check for input iterator errors
		if (!iter.status().ok()) {
			s = iter.status();
		}
		
		if (s.ok() && meta.fileSize > 0) {
			//Keep it
		} else {
			env.deleteFile(fname);
		}
		
		return s;
	}
}
