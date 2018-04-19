/**
 * Copyright (c) 2017-2018 Teng Huang <ht201509 at 163 dot com>
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
 * 
 * This file is translated from source code file Copyright (c) 2011 The 
 * LevelDB Authors and licensed under the BSD-3-Clause license.
 */

package com.tchaicatkovsky.jleveldb;

import com.tchaicatkovsky.jleveldb.util.Long0;
import com.tchaicatkovsky.jleveldb.util.Object0;
import com.tchaicatkovsky.jleveldb.util.SliceFactory;

public class FileName {

	static String makeFileName(String name, long number, String suffix) {
		return name + String.format("/%06d.%s", number, suffix);
	}

	// Return the name of the log file with the specified number
	// in the db named by "dbname". The result will be prefixed with
	// "dbname".
	public static String getLogFileName(String name, long number) {
		assert (number > 0);
		return makeFileName(name, number, "log");
	}

	// Return the name of the sstable with the specified number
	// in the db named by "dbname". The result will be prefixed with
	// "dbname".
	public static String getTableFileName(String name, long number) {
		assert (number > 0);
		return makeFileName(name, number, "ldb");
	}

	// Return the legacy file name for an sstable with the specified number
	// in the db named by "dbname". The result will be prefixed with
	// "dbname".
	public static String getSSTTableFileName(String name, long number) {
		assert (number > 0);
		return makeFileName(name, number, "sst");
	}

	// Return the name of the descriptor file for the db named by
	// "dbname" and the specified incarnation number. The result will be
	// prefixed with "dbname".
	public static String getDescriptorFileName(String dbname, long number) {
		assert (number > 0);
		return dbname + String.format("/MANIFEST-%06d", number);
	}

	// Return the name of the current file. This file contains the name
	// of the current manifest file. The result will be prefixed with
	// "dbname".
	public static String getCurrentFileName(String dbname) {
		return dbname + "/CURRENT";
	}

	// Return the name of the lock file for the db named by
	// "dbname". The result will be prefixed with "dbname".
	public static String getLockFileName(String dbname) {
		return dbname + "/LOCK";
	}

	// Return the name of a temporary file owned by the db named "dbname".
	// The result will be prefixed with "dbname".
	public static String getTempFileName(String dbname, long number) {
		assert (number > 0);
		return makeFileName(dbname, number, "dbtmp");
	}

	// Return the name of the info log file for "dbname".
	public static String getInfoLogFileName(String dbname) {
		return dbname + "/LOG";
	}

	// Return the name of the old info log file for "dbname".
	public static String getOldInfoLogFileName(String dbname) {
		return dbname + "/LOG.old";
	}

	public static boolean consumeDecimalNumber(Object0<String> in, Long0 val) {
		long v = 0;
		int digits = 0;
		char[] ary = in.getValue().toCharArray();
		int i = 0;
		for (; i < ary.length; i++) {
			char c = ary[i];
			if (c >= '0' && c <= '9') {
				++digits;
				int delta = (c - '0');
				if (v > Long.MAX_VALUE / 10 || (v == Long.MAX_VALUE / 10 && delta > Long.MAX_VALUE % 10)) {
					// Overflow
					return false;
				}
				v = (v * 10) + delta;
			} else {
				break;
			}
		}
		val.setValue(v);
		in.setValue(new String(ary, i, ary.length - i));
		return (digits > 0);
	}

	// If filename is a leveldb file, store the type of the file in *type.
	// The number encoded in the filename is stored in *number. If the
	// filename was successfully parsed, returns true. Else return false.
	// Owned filenames have the form:
	// dbname/CURRENT
	// dbname/LOCK
	// dbname/LOG
	// dbname/LOG.old
	// dbname/MANIFEST-[0-9]+
	// dbname/[0-9]+.(log|sst|ldb)
	public static boolean parseFileName(String fname, Long0 number, Object0<FileType> type) {
		number.setValue(0);
		type.setValue(null);
		String rest = fname;
		if (fname.equals("CURRENT")) {
			number.setValue(0);
			type.setValue(FileType.CurrentFile);
		} else if (fname.equals("LOCK")) {
			number.setValue(0);
			type.setValue(FileType.DBLockFile);
		} else if (fname.equals("LOG") || fname.equals("LOG.old")) {
			number.setValue(0);
			type.setValue(FileType.InfoLogFile);
		} else if (fname.startsWith("MANIFEST-")) {
			rest = rest.substring("MANIFEST-".length());
			Long0 num = new Long0();
			Object0<String> rest0 = new Object0<String>();
			rest0.setValue(rest);
			if (!consumeDecimalNumber(rest0, num)) {
				return false;
			}
			rest = rest0.getValue();
			if (!rest.isEmpty()) {
				return false;
			}
			type.setValue(FileType.DescriptorFile);
			number.setValue(num.getValue());
		} else {
			// Avoid strtoull() to keep filename format independent of the
			// current locale
			Long0 num = new Long0();
			Object0<String> rest0 = new Object0<String>();
			rest0.setValue(rest);
			if (!consumeDecimalNumber(rest0, num)) {
				return false;
			}
			rest = rest0.getValue();
			String suffix = rest;
			if (suffix.equals(".log")) {
				type.setValue(FileType.LogFile);
			} else if (suffix.equals(".sst") || suffix.equals(".ldb")) {
				type.setValue(FileType.TableFile);
			} else if (suffix.equals(".dbtmp")) {
				type.setValue(FileType.TempFile);
			} else {
				return false;
			}
			number.setValue(num.getValue());
		}
		return true;
	}

	// Make the CURRENT file point to the descriptor file with the
	// specified number.
	public static Status setCurrentFile(Env env, String dbname, long descriptorNumber) {
		// Remove leading "dbname/" and add newline to manifest file name
		String manifest = getDescriptorFileName(dbname, descriptorNumber);
		assert (manifest.startsWith(dbname + "/"));
		String contents = manifest.substring(dbname.length() + 1);
		String tmp = getTempFileName(dbname, descriptorNumber);
		Status s = env.writeStringToFileSync(SliceFactory.newUnpooled(contents + "\n"), tmp);
		if (s.ok()) {
			s = env.renameFile(tmp, getCurrentFileName(dbname));
		}
		if (!s.ok()) {
			env.deleteFile(tmp);
		}
		return s;
	}
}
