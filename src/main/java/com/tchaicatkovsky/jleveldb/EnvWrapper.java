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

package com.tchaicatkovsky.jleveldb;

import java.util.ArrayList;
import java.util.List;

import com.tchaicatkovsky.jleveldb.util.ByteBuf;
import com.tchaicatkovsky.jleveldb.util.Long0;
import com.tchaicatkovsky.jleveldb.util.Object0;
import com.tchaicatkovsky.jleveldb.util.Slice;

/**
 * An implementation of Env that forwards all calls to another Env. May be useful to clients who wish to override just part of the functionality of another Env.
 * 
 * @author thuang
 */
public class EnvWrapper implements Env {
	Env target;

	public EnvWrapper(Env env) {
		target = env;
	}

	public Env target() {
		return target;
	}
	
	public void setTarget(Env target) {
		this.target = target;
	}

	@Override
	public Status newSequentialFile(String fname, Object0<SequentialFile> result) {
		return target.newSequentialFile(fname, result);
	}

	@Override
	public Status newRandomAccessFile(String fname, Object0<RandomAccessFile0> result) {
		return target.newRandomAccessFile(fname, result);
	}

	@Override
	public Status newWritableFile(String fname, Object0<WritableFile> result) {
		return target.newWritableFile(fname, result);
	}

	@Override
	public Status newAppendableFile(String fname, Object0<WritableFile> result) {
		return target.newAppendableFile(fname, result);
	}

	@Override
	public boolean fileExists(String fname) {
		return target.fileExists(fname);
	}

	@Override
	public Status getChildren(String dir, List<String> result) {
		return target.getChildren(dir, result);
	}

	@Override
	public Status deleteFile(String fname) {
		return target.deleteFile(fname);
	}

	@Override
	public Status createDir(String dirname) {
		return target.createDir(dirname);
	}

	@Override
	public Status deleteDir(String dirname) {
		return target.deleteDir(dirname);
	}

	@Override
	public Status getFileSize(String fname, Long0 fileSize) {
		return target.getFileSize(fname, fileSize);
	}

	@Override
	public Status renameFile(String src, String dest) {
		return target.renameFile(src, dest);
	}

	@Override
	public Status lockFile(String fname, Object0<FileLock0> lock) {
		return target.lockFile(fname, lock);
	}

	@Override
	public Status unlockFile(FileLock0 lock) {
		return target.unlockFile(lock);
	}

	@Override
	public void schedule(Runnable r) {
		target.schedule(r);
	}

	@Override
	public void startThread(Runnable runnable) {
		target.startThread(runnable);
	}

	@Override
	public Status newLogger(String fname, Object0<Logger0> logger) {
		return target.newLogger(fname, logger);
	}

	@Override
	public Status getTestDirectory(Object0<String> path) {
		return target.getTestDirectory(path);
	}
	
	@Override
	public long nowMillis() {
		return target.nowMillis();
	}

	@Override
	public void sleepForMilliseconds(int millis) {
		target.sleepForMilliseconds(millis);
	}

	@Override
	public Status writeStringToFile(Slice data, String fname) {
		return target.writeStringToFile(data, fname);
	}

	@Override
	public Status writeStringToFileSync(Slice data, String fname) {
		return target.writeStringToFileSync(data, fname);
	}

	@Override
	public Status readFileToString(String fname, ByteBuf data) {
		return target.readFileToString(fname, data);
	}
	
	@Override
	public ArrayList<String> Test_getUnclosedFileList() {
		return target.Test_getUnclosedFileList();
	}
	
	@Override
	public void Test_printFileOpList() {
		target.Test_printFileOpList();
	}
	
	@Override
	public void Test_clearFileOpList() {
		target.Test_clearFileOpList();
	}
	
	@Override
	public Env clone() {
		return new EnvWrapper(target.clone());
	}
}
