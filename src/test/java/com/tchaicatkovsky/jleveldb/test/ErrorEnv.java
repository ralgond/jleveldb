/**
 * Copyright (c) 2017-2018, Teng Huang <ht201509 at 163 dot com>
 * All rights reserved.
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
