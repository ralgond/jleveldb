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

package com.tchaicatkovsky.jleveldb.db.format;

import java.util.List;

import com.tchaicatkovsky.jleveldb.FilterPolicy;
import com.tchaicatkovsky.jleveldb.util.ByteBuf;
import com.tchaicatkovsky.jleveldb.util.Slice;

/**
 * Filter policy wrapper that converts from internal keys to user keys
 */
public class InternalFilterPolicy extends FilterPolicy {
	
	FilterPolicy userPolicy;
	
	public InternalFilterPolicy(FilterPolicy userPolicy) {
		this.userPolicy = userPolicy;
	}
	
	public String name() {
		return userPolicy.name();
	}
	
	public void createFilter(List<Slice> keys, ByteBuf dst) {
		// We rely on the fact that the code in table.cc does not mind us
		// adjusting keys[].
		int n = keys.size();
		for (int i = 0; i < n; i++) {
			keys.set(i, DBFormat.extractUserKey(keys.get(i)));
		    // TODO(sanjay): Suppress dups?
		}
		userPolicy.createFilter(keys, dst);
	}
	
	public boolean keyMayMatch(Slice key, Slice f) {
		return userPolicy.keyMayMatch(DBFormat.extractUserKey(key), f);
	}
	
	public void delete() {
		if (userPolicy != null)
			userPolicy.delete();
	}
}
