package org.ht.jleveldb.db;

import org.ht.jleveldb.Env;
import org.ht.jleveldb.FileName;
import org.ht.jleveldb.Iterator0;
import org.ht.jleveldb.Options;
import org.ht.jleveldb.ReadOptions;
import org.ht.jleveldb.Status;
import org.ht.jleveldb.WritableFile;
import org.ht.jleveldb.table.TableBuilder;
import org.ht.jleveldb.util.Object0;
import org.ht.jleveldb.util.Slice;
import org.ht.jleveldb.util.Strings;

//Build a Table file from the contents of *iter.  The generated file
//will be named according to meta->number.  On success, the rest of
//*meta will be filled with metadata about the generated table.
//If no data is present in *iter, meta->file_size will be set to
//zero, and no Table file will be produced.
public class Builder {
	public static Status buildTable(String dbname, Env env, Options options, 
			TableCache tcache, Iterator0 iter, FileMetaData meta) {
		
		//System.out.println("[DEBUG] Builder.buildTable 1");
		Status s = null;
		meta.fileSize = 0;
		iter.seekToFirst();
		
		String fname = FileName.getTableFileName(dbname, meta.number);
		if (iter.valid()) {
			Object0<WritableFile> fileFuncOut = new Object0<>();
			s = env.newWritableFile(fname, fileFuncOut);
			//System.out.printf("[DEBUG] Builder.buildTable 2.0, fname=%s, s=%s\n", fname, s);
			if (!s.ok())
				return s;
			
			System.out.println("[DEBUG] Builder.buildTable 2.1");
			
			WritableFile file = fileFuncOut.getValue();
			TableBuilder builder = new TableBuilder(options, file);
			//System.out.println("[DEBUG] Builder.buildTable 2.2");
			meta.smallest.decodeFrom(iter.key());
			//System.out.println("[DEBUG] Builder.buildTable 2.3");
			for (; iter.valid(); iter.next()) {
				Slice key = iter.key();
				meta.largest.decodeFrom(key);
				//System.out.printf("[DEBUG] Builder.buildTable 2.4.1, key=%s, value=%s\n",
				//		Strings.escapeString(key), Strings.escapeString(iter.value()));
				builder.add(key, iter.value());
				//System.out.println("[DEBUG] Builder.buildTable 2.4.2");
			}
			
			//System.out.println("[DEBUG] Builder.buildTable 3");
			
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
			builder = null;
			
			//System.out.println("[DEBUG] Builder.buildTable 4");
			
			// Finish and check for file errors
			if (s.ok()) {
				s = file.sync();
			}
			if (s.ok()) {
				s = file.close();
			}
			file = null;
			
			//System.out.println("[DEBUG] Builder.buildTable 5");
			
			if (s.ok()) {
				// Verify that the table is usable
				Iterator0 it = tcache.newIterator(new ReadOptions(), meta.number, meta.fileSize);
				s = it.status();
				it.delete();
				it = null;
			}
		}
		
		//System.out.println("[DEBUG] Builder.buildTable 10");
		
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
