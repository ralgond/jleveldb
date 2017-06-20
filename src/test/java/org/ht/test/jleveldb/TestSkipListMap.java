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
package org.ht.test.jleveldb;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.Map;

import org.ht.jleveldb.db.SkipListMap;
import org.ht.jleveldb.db.SkipListMapComparator;
import org.ht.jleveldb.db.SkipListMap.Node;
import org.ht.jleveldb.util.ReflectionUtil;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Teng Huang ht201509@163.com
 */

public class TestSkipListMap {
	
	private static Logger logger = LoggerFactory.getLogger(TestSkipListMap.class);
	
	
	protected <K,V> void printNode(int maxLevel, Node<K,V> node) {
		logger.info("[BEGIN] node={}", node);
		for (int l = 0; l < maxLevel; l++)
			logger.info("[LEVEL{}] next:{} prev:{}", l, node.next(l), node.prev(l));
		logger.info("\n", node);
	}
	
	protected <K,V> void assertNodeNextPrev(int maxLevel, Node<K,V> node, Object... args) {
		for (int l = 0; l < maxLevel; l++) {
			assertTrue(node.next(l) == args[l]);
			assertTrue(node.prev(l) == args[l+maxLevel]);
		}
	}
	
	@SuppressWarnings("unchecked")
	protected <K,V> void assertGetNull(SkipListMap<K,V> map, Object... objects) {
		for (int i = 0; i < objects.length; i++) {
			assertNull(map.get((K)objects[i]));
		}
	}
	@SuppressWarnings("unchecked")
	protected <K,V> void assertGetEquals(SkipListMap<K,V> map, Object... objects) {
		for (int i = 0; i < objects.length; i++) {
			assertTrue(ReflectionUtil.isEquals(map.get((K)objects[i]), objects[i]));
		}
	}
	
	protected void checkGet(SkipListMap<Integer,Integer> map) {
		assertGetNull(map, 0, 1, 2, 4, 5, 7, 8, 10, 11, 13, 14, 16, 17, 19);
		
		assertGetEquals(map, 3, 6, 9, 12, 15, 18);
	}
	
	protected <K,V> void checkIterator(SkipListMap<K,V> map, Object... objects) {
		int idx = 0;
		Iterator<Map.Entry<K, V>> it = map.iterator();
		while (it.hasNext()) {
			Map.Entry<K, V> e = it.next();
			assertTrue(ReflectionUtil.isEquals(e.getKey(), objects[idx]));
			idx++;
		}
	}
	
	SkipListMapComparator<Integer> comp = new SkipListMapComparator<Integer>() {
		@Override
		final public int compare(Integer k1, Integer k2) {
			return k1.compareTo(k2);
		}
	};
	
	SkipListMap.Node<Integer, Integer> node3= new SkipListMap.Node<>(2,3,3);
	SkipListMap.Node<Integer, Integer> node6= new SkipListMap.Node<>(3,6,6);
	SkipListMap.Node<Integer, Integer> node9= new SkipListMap.Node<>(4,9,9);
	SkipListMap.Node<Integer, Integer> node12= new SkipListMap.Node<>(1,12,12);
	SkipListMap.Node<Integer, Integer> node15= new SkipListMap.Node<>(3,15,15);
	SkipListMap.Node<Integer, Integer> node18= new SkipListMap.Node<>(4,18,18);
	
	public SkipListMap<Integer,Integer> createSkipList00()  throws Exception {
		SkipListMap<Integer,Integer> map = new SkipListMap<>(5);
		map.setComparator(comp);
		
		map.put(3, 3);
		map.put(6, 6);
		map.put(9, 9);
		map.put(12, 12);
		map.put(15, 15);
		map.put(18, 18);
		
		checkIterator(map, 3, 6, 9, 12, 15, 18);
		
		return map;
	}
	
	@SuppressWarnings("unchecked")
	public SkipListMap<Integer,Integer> createSkipList01() throws Exception {
		SkipListMap<Integer,Integer> map = new SkipListMap<>(5);
		map.setComparator(comp);
		map.put(node3);
		map.put(node6);
		map.put(node9);
		map.put(node12);
		map.put(node15);
		map.put(node18);
	
		SkipListMap.Node<Integer, Integer> head = (SkipListMap.Node<Integer, Integer>)ReflectionUtil.getValue(map, "head");

//		printNode(5,head);
//		printNode(5,node3);
//		printNode(5,node6);
//		printNode(5,node9);
//		printNode(5,node12);
//		printNode(5,node15);
//		printNode(5,node18);
		
		assertNodeNextPrev(5, head, node3, node3, node6, node9,				null, null, null, null, null, null);
		assertNodeNextPrev(5, node3, node6, node6, null, null, null, 		head, head, null, null, null);
		assertNodeNextPrev(5, node6, node9, node9, node9, null, null, 		node3, node3, head, null, null);
		assertNodeNextPrev(5, node9, node12, node15, node15, node18, null, 	node6, node6, node6, head, null);
		assertNodeNextPrev(5, node12, node15, null, null, null, null, 		node9, null, null, null, null);
		assertNodeNextPrev(5, node15, node18, node18, node18, null, null, 	node12, node9, node9, null, null);
		assertNodeNextPrev(5, node18, null, null, null, null, null, 		node15, node15, node15, node9, null);
		
		checkIterator(map, 3, 6, 9, 12, 15, 18);
		
		return map;
	}

	
	@SuppressWarnings("unchecked")
	public SkipListMap<Integer,Integer> createSkipList02() throws Exception {	
		SkipListMap<Integer,Integer> map = new SkipListMap<>(5);
		map.setComparator(comp);
		map.put(node18);
		map.put(node15);
		map.put(node12);
		map.put(node9);
		map.put(node6);
		map.put(node3);
		
		SkipListMap.Node<Integer, Integer> head = (SkipListMap.Node<Integer, Integer>)ReflectionUtil.getValue(map, "head");
		
		assertNodeNextPrev(5, head, node3, node3, node6, node9,				null, null, null, null, null, null);
		assertNodeNextPrev(5, node3, node6, node6, null, null, null, 		head, head, null, null, null);
		assertNodeNextPrev(5, node6, node9, node9, node9, null, null, 		node3, node3, head, null, null);
		assertNodeNextPrev(5, node9, node12, node15, node15, node18, null, 	node6, node6, node6, head, null);
		assertNodeNextPrev(5, node12, node15, null, null, null, null, 		node9, null, null, null, null);
		assertNodeNextPrev(5, node15, node18, node18, node18, null, null, 	node12, node9, node9, null, null);
		assertNodeNextPrev(5, node18, null, null, null, null, null, 		node15, node15, node15, node9, null);
		
		checkIterator(map, 3, 6, 9, 12, 15, 18);
		
		return map;
	}
	
	@SuppressWarnings("unchecked")
	public SkipListMap<Integer,Integer> createSkipList03() throws Exception {		
		SkipListMap<Integer,Integer> map = new SkipListMap<>(5);
		map.setComparator(comp);
		map.put(node9);
		map.put(node18);
		map.put(node12);
		map.put(node6);
		map.put(node15);
		map.put(node3);
		
		SkipListMap.Node<Integer, Integer> head = (SkipListMap.Node<Integer, Integer>)ReflectionUtil.getValue(map, "head");
		
		assertNodeNextPrev(5, head, node3, node3, node6, node9,				null, null, null, null, null, null);
		assertNodeNextPrev(5, node3, node6, node6, null, null, null, 		head, head, null, null, null);
		assertNodeNextPrev(5, node6, node9, node9, node9, null, null, 		node3, node3, head, null, null);
		assertNodeNextPrev(5, node9, node12, node15, node15, node18, null, 	node6, node6, node6, head, null);
		assertNodeNextPrev(5, node12, node15, null, null, null, null, 		node9, null, null, null, null);
		assertNodeNextPrev(5, node15, node18, node18, node18, null, null, 	node12, node9, node9, null, null);
		assertNodeNextPrev(5, node18, null, null, null, null, null, 		node15, node15, node15, node9, null);
		
		checkIterator(map, 3, 6, 9, 12, 15, 18);
		
		return map;
	}
	
	@SuppressWarnings("unchecked")
	public SkipListMap<Integer,Integer> createSkipList04() throws Exception {
		SkipListMap<Integer,Integer> map = new SkipListMap<>(5);
		map.setComparator(comp);
		map.put(node18);
		map.put(node9);
		map.put(node12);
		map.put(node6);
		map.put(node15);
		map.put(node3);
		
		SkipListMap.Node<Integer, Integer> head = (SkipListMap.Node<Integer, Integer>)ReflectionUtil.getValue(map, "head");

		assertNodeNextPrev(5, head, node3, node3, node6, node9,				null, null, null, null, null, null);
		assertNodeNextPrev(5, node3, node6, node6, null, null, null, 		head, head, null, null, null);
		assertNodeNextPrev(5, node6, node9, node9, node9, null, null, 		node3, node3, head, null, null);
		assertNodeNextPrev(5, node9, node12, node15, node15, node18, null, 	node6, node6, node6, head, null);
		assertNodeNextPrev(5, node12, node15, null, null, null, null, 		node9, null, null, null, null);
		assertNodeNextPrev(5, node15, node18, node18, node18, null, null, 	node12, node9, node9, null, null);
		assertNodeNextPrev(5, node18, null, null, null, null, null, 		node15, node15, node15, node9, null);

		checkIterator(map, 3, 6, 9, 12, 15, 18);
		
		return map;
	}
	

	
	@Test
	public void testGet() throws Exception {
		checkGet(createSkipList00());
		checkGet(createSkipList01());
		checkGet(createSkipList02());
		checkGet(createSkipList03());
		checkGet(createSkipList04());
	}
	
	@Test 
	public void testRemove01() throws Exception {
		SkipListMap<Integer, Integer> map = createSkipList00();
		map.remove(3);
		checkIterator(map, 6, 9, 12, 15, 18);
		map.remove(6);
		checkIterator(map, 9, 12, 15, 18);
		map.remove(9);
		checkIterator(map, 12, 15, 18);
		map.remove(12);
		checkIterator(map, 15, 18);
		map.remove(15);
		checkIterator(map, 18);
		map.remove(18);
		assertTrue(!map.iterator().hasNext());
		assertTrue(map.size() == 0);
	}
	
	@Test 
	public void testRemove02() throws Exception {
		SkipListMap<Integer, Integer> map = createSkipList00();
		map.remove(18);
		checkIterator(map, 3, 6, 9, 12, 15);
		map.remove(15);
		checkIterator(map, 3, 6, 9, 12);
		map.remove(12);
		checkIterator(map, 3, 6, 9);
		map.remove(9);
		checkIterator(map, 3, 6);
		map.remove(6);
		checkIterator(map, 3);
		map.remove(3);
		assertTrue(!map.iterator().hasNext());
		assertTrue(map.size() == 0);
	}
	
	@Test 
	public void testRemove03() throws Exception {
		SkipListMap<Integer, Integer> map = createSkipList01();
		map.remove(9);
		checkIterator(map, 3, 6, 12, 15, 18);
		map.remove(6);
		checkIterator(map, 3, 12, 15, 18);
		map.remove(12);
		checkIterator(map, 3, 15, 18);
		map.remove(3);
		checkIterator(map, 15, 18);
		map.remove(18);
		checkIterator(map, 15);
		map.remove(15);
		assertTrue(!map.iterator().hasNext());
		assertTrue(map.size() == 0);
	}
	
	@Test
	public void testFristLastEntry() throws Exception {
		SkipListMap<Integer, Integer> map = createSkipList01();
		assertTrue(map.firstEntry().getKey() == 3);
		assertTrue(map.lastEntry().getKey() == 18);
		
		map.remove(3);
		assertTrue(map.firstEntry().getKey() == 6);
		map.remove(18);
		assertTrue(map.lastEntry().getKey() == 15);
		map.remove(9);
		assertTrue(map.lastEntry().getKey() == 15);
		map.remove(15);
		assertTrue(map.lastEntry().getKey() == 12);
		map.remove(12);
		assertTrue(map.lastEntry().getKey() == 6);
		map.remove(6);
		
		assertNull(map.firstEntry());
		assertNull(map.lastEntry());
	}
}
