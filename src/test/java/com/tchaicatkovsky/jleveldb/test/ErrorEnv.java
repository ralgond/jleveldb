package com.tchaicatkovsky.jleveldb.test;

import com.tchaicatkovsky.jleveldb.Env;
import com.tchaicatkovsky.jleveldb.EnvWrapper;
import com.tchaicatkovsky.jleveldb.LevelDB;
import com.tchaicatkovsky.jleveldb.Status;
import com.tchaicatkovsky.jleveldb.WritableFile;
import com.tchaicatkovsky.jleveldb.util.Object0;

public class ErrorEnv extends EnvWrapper {
	 boolean writableFileError;
	 int numWritableFileErrors;

	 public ErrorEnv() {
		 super(LevelDB.defaultEnv());

         writableFileError = false;
         numWritableFileErrors = 0;
	 }

	 public Status newWritableFile(String fname, Object0<WritableFile> result) {
	    if (writableFileError) {
	    	++numWritableFileErrors;
	    	result.setValue(null);
	    	return Status.ioError(fname+" fake error");
	    }
	    return target().newWritableFile(fname, result);
	 }

	 public Status newAppendableFile(String fname, Object0<WritableFile> result) {
	    if (writableFileError) {
	    	++numWritableFileErrors;
	    	result.setValue(null);
	    	return Status.ioError(fname+" fake error");
	    }
	    return target().newAppendableFile(fname, result);
	}
	 
	@Override
	public Env clone() {
		ErrorEnv ret = new ErrorEnv();
		ret.writableFileError = writableFileError;
		ret.numWritableFileErrors = numWritableFileErrors;
		return ret;
	}
}
