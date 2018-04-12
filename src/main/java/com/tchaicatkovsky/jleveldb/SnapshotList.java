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

public class SnapshotList {
	
	public SnapshotList() {
		list.next = list;
		list.prev = list;
	}
	
	public boolean isEmpty() {
		return list.next == list;
	}
	
	public Snapshot oldest() {
		assert(!isEmpty());
		return list.next;
	}
	
	public Snapshot newest() {
		assert(!isEmpty());
		return list.prev;
	}
	
	public Snapshot new0(long seq) {
		Snapshot s = new Snapshot(seq, this);
		s.next = list;
		s.prev = list.prev;
		s.prev.next = s;
		s.next.prev = s;
		return s;
	}
	
	public void delete(Snapshot s) {
		assert(s.list == this);
		s.prev.next = s.next;
		s.next.prev=  s.prev;
	}
	
	Snapshot list = new Snapshot();
}
