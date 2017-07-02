package com.tchaicatkovsky.jleveldb.test;

import com.tchaicatkovsky.jleveldb.EnvWrapper;
import com.tchaicatkovsky.jleveldb.LevelDB;
import com.tchaicatkovsky.jleveldb.Status;
import com.tchaicatkovsky.jleveldb.WritableFile;
import com.tchaicatkovsky.jleveldb.util.Object0;

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
