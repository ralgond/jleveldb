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

public class ReadOptions {
	/**
	 * If true, all data read from underlying storage will be verified against corresponding checksums. Default: false
	 */
	public boolean verifyChecksums;

	/**
	 * Should the data read for this iteration be cached in memory? Callers may wish to set this field to false for bulk scans. Default: true
	 */
	public boolean fillCache;

	/**
	 * If "snapshot" is non-null, read as of the supplied snapshot (which must belong to the DB that is being read and which must not have been released). If "snapshot" is null, use an implicit
	 * snapshot of the state at the beginning of this read operation. Default: null
	 */
	public Snapshot snapshot;

	public ReadOptions() {
		verifyChecksums = false;
		fillCache = true;
		snapshot = null;
	}

	@Override
	public ReadOptions clone() {
		ReadOptions ro = new ReadOptions();
		ro.verifyChecksums = verifyChecksums;
		ro.fillCache = fillCache;
		ro.snapshot = snapshot;
		return ro;
	}
}
