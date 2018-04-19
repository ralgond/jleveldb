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
 */

package com.tchaicatkovsky.jleveldb.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class ListUtils {
	public static <T> int upperBound(ArrayList<T> l, T target, Comparator<T> cmp) {
		int offset = 0;
		int size = l.size();
		List<T> view = l.subList(offset, offset+size);
		int ret = Collections.binarySearch(view, target, cmp);
		while (ret >= 0) {
			offset += (ret + 1);
			size -= (ret + 1);
			view = l.subList(offset, offset+size);
			ret = Collections.binarySearch(view, target, cmp);
		}
		
		int insertionPoint = -1 * (ret + 1);
		
		return (insertionPoint == size ? l.size() : insertionPoint + offset);
	}
	
	public static<T> int lowerBound(ArrayList<T> l, T target, Comparator<T> cmp) {
		int size = l.size();
		List<T> view = l.subList(0, size);
		boolean found = false;
		int ret = Collections.binarySearch(view, target, cmp);
		int lastFoundIdx = -1;
		while (ret >= 0) {
			found = true;
			size = ret + 1;
			view = l.subList(0, size);
			lastFoundIdx = ret;
			ret = Collections.binarySearch(view, target, cmp);
			if (ret == lastFoundIdx) {
				break;
			}
		}
		
		if (found)
			return size - 1;
		else 
			return -1 * (ret + 1);
	}
}
