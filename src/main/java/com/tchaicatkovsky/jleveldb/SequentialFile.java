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

import com.tchaicatkovsky.jleveldb.util.Slice;

public interface SequentialFile {
	void delete();
	
	  // Read up to "n" bytes from the file.  "scratch[0..n-1]" may be
	  // written by this routine.  Sets "*result" to the data that was
	  // read (including if fewer than "n" bytes were successfully read).
	  // May set "*result" to point at data in "scratch[0..n-1]", so
	  // "scratch[0..n-1]" must be live when "*result" is used.
	  // If an error was encountered, returns a non-OK status.
	  //
	  // REQUIRES: External synchronization
	Status read(int n, Slice result, byte[] scratch);
	
	  // Skip "n" bytes from the file. This is guaranteed to be no
	  // slower that reading the same data, but may be faster.
	  //
	  // If end of file is reached, skipping will stop at the end of the
	  // file, and Skip will return OK.
	  //
	  // REQUIRES: External synchronization
	Status skip(long n);
}
