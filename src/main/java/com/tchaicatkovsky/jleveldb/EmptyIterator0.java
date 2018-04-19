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

import com.tchaicatkovsky.jleveldb.util.Slice;
import com.tchaicatkovsky.jleveldb.util.SliceFactory;

public class EmptyIterator0 extends Iterator0 {

	Status status;

	public EmptyIterator0(Status s) {
		status = s;
	}

	@Override
	public void delete() {
		super.delete();
	}

	@Override
	public boolean valid() {
		return false;
	}

	@Override
	public void seekToFirst() {

	}

	@Override
	public void seekToLast() {

	}

	@Override
	public void seek(Slice target) {

	}

	@Override
	public void next() {
		assert (false);
	}

	@Override
	public void prev() {
		assert (false);
	}

	@Override
	public Slice key() {
		assert (false);
		return SliceFactory.newUnpooled();
	}

	@Override
	public Slice value() {
		assert (false);
		return SliceFactory.newUnpooled();
	}

	@Override
	public Status status() {
		return status;
	}

}
