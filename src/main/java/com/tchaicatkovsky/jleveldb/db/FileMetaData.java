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

package com.tchaicatkovsky.jleveldb.db;

import com.tchaicatkovsky.jleveldb.db.format.InternalKey;

public class FileMetaData {
	public int refs;
	/**
	 * Seeks allowed until compaction
	 */
	int allowedSeeks;
	public long number;
	/**
	 * File size in bytes;
	 */
	public long fileSize;
	/**
	 * Smallest internal key served by table
	 */
	public InternalKey smallest = new InternalKey();
	/**
	 * Largest internal key served by table
	 */
	public InternalKey largest = new InternalKey();
	public int numEntries;
	
	public FileMetaData() {
		
	}
	
	public FileMetaData(long number, long fileSize, InternalKey smallest, InternalKey largest, int numEntries) {
		this.number = number;
		this.fileSize = fileSize;
		this.smallest = smallest;
		this.largest = largest;
		this.numEntries = numEntries;
	}
	
	public void delete() {

	}
	
	@Override
	public FileMetaData clone() {
		FileMetaData ret = new FileMetaData();
		ret.refs = refs;
		ret.allowedSeeks = allowedSeeks;
		ret.number = number;
		ret.fileSize = fileSize;
		if (smallest != null)
			ret.smallest = smallest.clone();
		if (largest != null)
			ret.largest = largest.clone();
		ret.numEntries = numEntries;
		return ret;
	}
	
	public String debugString() {
		String s = "{\n";
		s += ("\tnumber: " + number + "\n");
		s += ("\tfileSize: " + fileSize + "\n");
		s += ("\tsmallest: " + smallest.debugString() + "\n");
		s += ("\tlargest: " + largest.debugString() + "\n");
		s += ("\tnumEntries: " + numEntries + "\n}\n");
		return s;
	}
}
