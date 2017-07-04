/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tchaicatkovsky.jleveldb;

import com.tchaicatkovsky.jleveldb.util.Slice;

public interface RandomAccessFile0 {

	String name();

	void delete();

	/**
	 * Read up to "n" bytes from the file starting at "offset".</br>
	 * </br>
	 * 
	 * "scratch[0..n-1]" may be written by this routine. Sets "result" to the data that was read (including if fewer than "n" bytes were successfully read). May set "*result" to point at data in
	 * "scratch[0..n-1]", so "scratch[0..n-1]" must be live when "result" is used. If an error was encountered, returns a non-OK status.</br>
	 * </br>
	 * 
	 * Safe for concurrent use by multiple threads.
	 * 
	 * @param offset
	 * @param n
	 * @param result
	 * @param scratch
	 * @return
	 */
	Status read(long offset, int n, Slice result, byte[] scratch);

	void close();
}
