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

//Build a Table file from the contents of *iter.  The generated file
//will be named according to meta->number.  On success, the rest of
//*meta will be filled with metadata about the generated table.
//If no data is present in *iter, meta->file_size will be set to
//zero, and no Table file will be produced.
public class Builder {
	
	public static Status buildTable(String dbname, Env env, Options options, 
			TableCache tcache, Iterator0 iter, FileMetaData meta) {
		
		System.out.println("[DEBUG] Builder.buildTable 1");
		
		Status s = Status.ok0();
		meta.fileSize = 0;
		iter.seekToFirst();
		
		String fname = FileName.getTableFileName(dbname, meta.number);
		if (iter.valid()) {
			Object0<WritableFile> fileFuncOut = new Object0<>();
			s = env.newWritableFile(fname, fileFuncOut);
			if (!s.ok())
				return s;
			
			System.out.println("[DEBUG] Builder.buildTable 2");
			
			WritableFile file = fileFuncOut.getValue();
			TableBuilder builder = new TableBuilder(options, file);
			meta.smallest.decodeFrom(iter.key());
			for (; iter.valid(); iter.next()) {
				Slice key = iter.key();
				meta.largest.decodeFrom(key);
				builder.add(key, iter.value());
			}
			
			System.out.println("[DEBUG] Builder.buildTable 3");
			
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
			
			System.out.println("[DEBUG] Builder.buildTable 4");
			
			
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
			
			System.out.println("[DEBUG] Builder.buildTable 4.1");
			
			if (s.ok()) {
				// Verify that the table is usable
				Iterator0 it = tcache.newIterator(new ReadOptions(), meta.number, meta.fileSize);
				s = it.status();
				it.delete();
				it = null;
			}
			
			System.out.println("[DEBUG] Builder.buildTable 5");
		}
		
		// Check for input iterator errors
		if (!iter.status().ok()) {
			s = iter.status();
		}
		
		System.out.println("[DEBUG] Builder.buildTable 6 s="+s+", meta="+meta+", iter="+iter);
		
		if (s.ok() && meta.fileSize > 0) {
			//Keep it
		} else {
			env.deleteFile(fname);
		}
		
		return s;
	}
}
