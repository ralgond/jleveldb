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
 * This file is translated from source code file Copyright (c) 2011 
 * The LevelDB Authors and licensed under the BSD-3-Clause license.
 */

package com.tchaicatkovsky.jleveldb;

import com.tchaicatkovsky.jleveldb.util.Slice;

public abstract class Iterator0 {
	public void delete() {
		if (cleanup.runnable != null) {
			cleanup.runnable.run();
			for (Cleanup c = cleanup.next; c != null;) {
				c.runnable.run();
				c = c.next;
			}
		}
	}

	public void printCleaner() {

		Cleanup head = cleanup;
		while (head != null && head.runnable != null) {
			head = head.next;
		}
	}

	public abstract boolean valid();

	public abstract void seekToFirst();

	public abstract void seekToLast();

	public abstract void seek(Slice target);

	public abstract void next();

	public abstract void prev();

	public abstract Slice key();

	public abstract Slice value();

	public abstract Status status();

	public void registerCleanup(Runnable runnable) {
		if (cleanup.runnable == null) {
			cleanup.runnable = runnable;
		} else {
			Cleanup c = new Cleanup();
			c.runnable = runnable;

			c.next = cleanup;
			cleanup = c;
		}
	}

	static class Cleanup {
		Runnable runnable;
		Cleanup next;
	}

	Cleanup cleanup = new Cleanup();

	public static Iterator0 newEmptyIterator() {
		return new EmptyIterator0(Status.ok0());
	}

	public static Iterator0 newErrorIterator(Status status) {
		return new EmptyIterator0(status);
	}
}
