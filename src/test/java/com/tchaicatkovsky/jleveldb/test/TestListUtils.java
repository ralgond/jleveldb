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

package com.tchaicatkovsky.jleveldb.test;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Comparator;

import org.junit.Test;

import com.tchaicatkovsky.jleveldb.util.ListUtils;

public class TestListUtils {
	@Test
	public void testUpperBound() {
		ArrayList<Integer> l = new ArrayList<>();
		l.add(3);
		l.add(6);
		l.add(9);
		l.add(12);
		l.add(15);
		l.add(18);
		
		Comparator<Integer> cmp = new Comparator<Integer>() {
			public int compare(Integer a, Integer b) {
				return Integer.compare(a, b);
			}
		};
		
		assertEquals(ListUtils.upperBound(l, 0, cmp), 0);
		assertEquals(ListUtils.upperBound(l, 3, cmp), 1);
		assertEquals(ListUtils.upperBound(l, 4, cmp), 1);
		assertEquals(ListUtils.upperBound(l, 6, cmp), 2);
		assertEquals(ListUtils.upperBound(l, 7, cmp), 2);
		assertEquals(ListUtils.upperBound(l, 9, cmp), 3);
		assertEquals(ListUtils.upperBound(l, 10, cmp), 3);
		assertEquals(ListUtils.upperBound(l, 12, cmp), 4);
		assertEquals(ListUtils.upperBound(l, 13, cmp), 4);
		assertEquals(ListUtils.upperBound(l, 15, cmp), 5);
		assertEquals(ListUtils.upperBound(l, 16, cmp), 5);
		assertEquals(ListUtils.upperBound(l, 18, cmp), 6);
		assertEquals(ListUtils.upperBound(l, 19, cmp), 6);
		assertEquals(ListUtils.upperBound(l, 20, cmp), 6);
	}
	
	@Test
	public void testLowerBound() {
		ArrayList<Integer> l = new ArrayList<>();
		l.add(3);
		l.add(6);
		l.add(9);
		l.add(12);
		l.add(15);
		l.add(18);
		
		Comparator<Integer> cmp = new Comparator<Integer>() {
			public int compare(Integer a, Integer b) {
				return Integer.compare(a, b);
			}
		};

		assertEquals(ListUtils.lowerBound(l, 0, cmp), 0);
		assertEquals(ListUtils.lowerBound(l, 3, cmp), 0);
		assertEquals(ListUtils.lowerBound(l, 4, cmp), 1);
		assertEquals(ListUtils.lowerBound(l, 6, cmp), 1);
		assertEquals(ListUtils.lowerBound(l, 7, cmp), 2);
		assertEquals(ListUtils.lowerBound(l, 9, cmp), 2);
		assertEquals(ListUtils.lowerBound(l, 10, cmp), 3);
		assertEquals(ListUtils.lowerBound(l, 12, cmp), 3);
		assertEquals(ListUtils.lowerBound(l, 13, cmp), 4);
		assertEquals(ListUtils.lowerBound(l, 15, cmp), 4);
		assertEquals(ListUtils.lowerBound(l, 16, cmp), 5);
		assertEquals(ListUtils.lowerBound(l, 18, cmp), 5);
		assertEquals(ListUtils.lowerBound(l, 19, cmp), 6);
		assertEquals(ListUtils.lowerBound(l, 20, cmp), 6);
	}
	
	@Test
	public void testLowerBound01() {
		ArrayList<Integer> l = new ArrayList<>();
		l.add(3);
		l.add(3);
		l.add(3);
		l.add(6);
		l.add(6);
		l.add(6);
		
		Comparator<Integer> cmp = new Comparator<Integer>() {
			public int compare(Integer a, Integer b) {
				return Integer.compare(a, b);
			}
		};
		

		assertEquals(ListUtils.lowerBound(l, 0, cmp), 0);
		assertEquals(ListUtils.lowerBound(l, 2, cmp), 0);
		assertEquals(ListUtils.lowerBound(l, 3, cmp), 0);
		assertEquals(ListUtils.lowerBound(l, 4, cmp), 3);
		assertEquals(ListUtils.lowerBound(l, 6, cmp), 3);
		assertEquals(ListUtils.lowerBound(l, 7, cmp), 6);
	}
}
