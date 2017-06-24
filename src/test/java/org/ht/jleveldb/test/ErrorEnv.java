package org.ht.jleveldb.test;

import org.ht.jleveldb.EnvWrapper;
import org.ht.jleveldb.LevelDB;
import org.ht.jleveldb.Status;
import org.ht.jleveldb.WritableFile;
import org.ht.jleveldb.util.Object0;

public class ErrorEnv extends EnvWrapper {
	 boolean writable_file_error_;
	 int num_writable_file_errors_;

	 public ErrorEnv() {
		 super(LevelDB.defaultEnv());

         writable_file_error_ = false;
         num_writable_file_errors_ = 0;
	 }

	 public Status newWritableFile(String fname, Object0<WritableFile> result) {
	    if (writable_file_error_) {
	    	++num_writable_file_errors_;
	    	result.setValue(null);
	    	return Status.ioError(fname+" fake error");
	    }
	    return target().newWritableFile(fname, result);
	 }

	 public Status newAppendableFile(String fname, Object0<WritableFile> result) {
	    if (writable_file_error_) {
	    	++num_writable_file_errors_;
	    	result.setValue(null);
	    	return Status.ioError(fname+" fake error");
	    }
	    return target().newAppendableFile(fname, result);
	}
}
