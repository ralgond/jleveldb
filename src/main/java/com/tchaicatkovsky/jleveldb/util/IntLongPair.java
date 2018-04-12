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

package com.tchaicatkovsky.jleveldb.util;

public class IntLongPair {
	public int i;
	public long l;
	
	
	public IntLongPair() {
		
	}
	
	public IntLongPair(int i, long l) {
		this.i = i;
		this.l = l;
	}
	
	@Override
	public int hashCode() {
		int h = 0;
		h = h * 31 + Integer.hashCode(i);
		h = h * 31 + Long.hashCode(l);
		return h;
	}
	
	@Override
	public boolean equals(Object o) {
		IntLongPair p = (IntLongPair)o;
		return i == p.i && l == p.l;
	}
}
