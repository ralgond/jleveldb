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

import java.util.ArrayList;

import org.junit.Test;

import com.tchaicatkovsky.jleveldb.FileName;
import com.tchaicatkovsky.jleveldb.FileType;
import com.tchaicatkovsky.jleveldb.util.Long0;
import com.tchaicatkovsky.jleveldb.util.Object0;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;

public class TestFileName {
	static class ParseResult {
		String fname;
		long number;
		FileType type;
		
		public ParseResult(String fname, long number, FileType type) {
			this.fname = fname;
			this.number = number;
			this.type = type;
		}
	}
	
	@Test
	public void testParse() {
		Object0<FileType> type0 = new Object0<FileType>();
		Long0 number0 = new Long0();

		  // Successful parses
		  ArrayList<ParseResult> cases = new ArrayList<>();
		  cases.add(new ParseResult("100.log", 100, FileType.LogFile));
		  cases.add(new ParseResult("0.log", 0, FileType.LogFile));
		  cases.add(new ParseResult("0.sst", 0, FileType.TableFile));
		  cases.add(new ParseResult("0.ldb", 0, FileType.TableFile));
		  cases.add(new ParseResult("CURRENT", 0, FileType.CurrentFile));
		  cases.add(new ParseResult("LOCK", 0, FileType.DBLockFile));
		  cases.add(new ParseResult("MANIFEST-2", 2, FileType.DescriptorFile));
		  cases.add(new ParseResult("MANIFEST-7", 7, FileType.DescriptorFile));
		  cases.add(new ParseResult("LOG", 0, FileType.InfoLogFile));
		  cases.add(new ParseResult("LOG.old", 0, FileType.InfoLogFile));
		  cases.add(new ParseResult("8446744073709551615.log", 8446744073709551615L, FileType.LogFile));

		  for (int i = 0; i < cases.size(); i++) {
		    String f = cases.get(i).fname;
		    assertTrue(FileName.parseFileName(f, number0, type0));
		    assertTrue(cases.get(i).type == type0.getValue());
		    assertEquals(cases.get(i).number, number0.getValue());
		  }

		  // Errors
		  String errors[] = new String[] {
		    "",
		    "foo",
		    "foo-dx-100.log",
		    ".log",
		    "",
		    "manifest",
		    "CURREN",
		    "CURRENTX",
		    "MANIFES",
		    "MANIFEST",
		    "MANIFEST-",
		    "XMANIFEST-3",
		    "MANIFEST-3x",
		    "LOC",
		    "LOCKx",
		    "LO",
		    "LOGx",
		    "18446744073709551616.log",
		    "184467440737095516150.log",
		    "100",
		    "100.",
		    "100.lop"
		  };
		  
		  for (int i = 0; i < errors.length; i++) {
			  String f = errors[i];
			  assertFalse(FileName.parseFileName(f, number0, type0));
		  }
	}
}
