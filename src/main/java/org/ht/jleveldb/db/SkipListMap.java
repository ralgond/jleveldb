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
package org.ht.jleveldb.db;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

/**
 * @author Teng Huang ht201509@163.com
 */
public class SkipListMap<K, V> {

	//private static Logger logger = LoggerFactory.getLogger(SkipListMap.class);
	
	ThreadLocalRandom rnd = ThreadLocalRandom.current();
	Comparator<K> comp;
	final int maxLevel;
	final Node<K,V> head;
	Node<K,V> tail;
	
	int size;
	int branching = 4;
	
	public static class Node<K1,V1> implements Map.Entry<K1, V1>{
		K1 key;
		V1 value;
		int level;
		Object[] link;
		
		public Node(int level) {
			this.level = level;
			link = new Object[level*2]; //[0 - level/2] is next link, [level/2+1,level*2-1] is prev link.
		}
		
		public Node(int level, K1 k, V1 v) {
			this(level);
			key = k;
			value = v;
		}
		
		@Override
		public V1 getValue() {
			return value;
		}
		
		@Override
		public K1 getKey() {
			return key;
		}
		
		@Override
		public V1 setValue(V1 v) {
			V1 old = value;
			value = v;
			return old;
		}
		
		@SuppressWarnings("unchecked")
		@Override
		public boolean equals(Object o) {
			// Why need this method?
			Node<K1,V1> e2 = (Node<K1,V1>)o;
			return (getKey()==null ?
					e2.getKey()==null : getKey().equals(e2.getKey()))  &&
				    (getValue()==null ?
				    e2.getValue()==null : getValue().equals(e2.getValue()));
		}
		
		@Override
		public int hashCode() {
			return (getKey()==null ? 0 : getKey().hashCode()) ^ (getValue()==null ? 0 : getValue().hashCode());
		}
		 
		@Override
		public String toString() {
			return String.format("{key:%s,level:%d}", key, level);
		}
		
		@SuppressWarnings("unchecked")
		final public Node<K1,V1> prev(int l) {
			return l < level ? (Node<K1,V1>)link[l+level] : null;
		}
		
		final public void setPrev(int l, Node<K1,V1> n) {
			link[l+level] = n;
		}
		
		@SuppressWarnings("unchecked")
		final public Node<K1,V1> next(int l) {
			return l < level ? (Node<K1,V1>)link[l] : null;
		}
		
		final public void setNext(int l, Node<K1,V1> n) {
			link[l] = n;
		}
		
		public void addNext(Node<K1,V1> nextNode) {
			Objects.requireNonNull(nextNode);
			
			int l = 0; 
			
			Node<K1,V1> prev = this;
			
			while (l < nextNode.level && prev != null) {
				//logger.debug("[addNext], l={}, nextNode={}, prev={}", l, nextNode, prev);
				
				if (prev.level <= l) {
					prev = prev.prev(l-1);
					continue;
				}

				nextNode.setNext(l, prev.next(l)); // nextNode.next := prev.next
				prev.setNext(l, nextNode); 	       // prev.next := nextNode
				if (nextNode.next(l) != null)
					nextNode.next(l).setPrev(l, nextNode); //nextNode.next.prev := nextNode;
				nextNode.setPrev(l, prev); //nextNode.prev := prev;
				
				l++;
			}
		}
		
		public void remove() {
			for (int l = 0; l < level; l++) {
				Node<K1,V1> prev = prev(l);
				prev.setNext(l, next(l)); //prev.next := this.next;
				if (prev.next(l) != null)
					prev.next(l).setPrev(l, prev); //prev.next.prev := prev;
			}
		}
	}
	
	public SkipListMap(int maxLevel) {
		this.maxLevel = maxLevel;
		this.head = new Node<K,V>(maxLevel);
		this.tail = head;
	}
	
	public SkipListMap(int maxLevel, int branching, Comparator<K> comp) {
		this(maxLevel);
		this.branching = branching;
		this.comp = comp;
	}

	public void setComparator(Comparator<K> comp) {
		this.comp = comp;
	}
	
	final int randomLevel() {
		int level = 1;
		while (rnd.nextInt() % branching == 0) {
			level++;
			if (level >= maxLevel)
				break;
		}
		return level;
	}
	
	public V get(K k) {
		FindResult<K,V> result = find(k);
		//logger.debug("[findNode] node={}, k={}", node, k);
		return result.node == null ? null : result.node.value;
	}
	
	final static class FindResult<K1,V1> {
		Node<K1,V1> prev;
		Node<K1,V1> node;
		boolean found;
	}
	
	public FindResult<K,V> findFirstGreaterOrEqual(K target) {
		Objects.requireNonNull(target);
		
		FindResult<K,V> result = new FindResult<K,V>();
		
		Node<K,V> prev = head;
		
		int l = prev.level - 1;
		while (l >= 0 && prev != null) {
			Node<K,V> next = prev.next(l);
			if (next == null) {
				l--;
				continue;
			}
			
			int cmpRes = comp.compare(next.key, target);
			if (cmpRes == 0) {
				//System.out.println("skiplist.find: target="+k+" == next.key="+next.key);
				result.prev = prev;
				result.node = next;
				result.found = true;
				return result;
			} else if (cmpRes < 0) {
				//System.out.println("skiplist.find: target="+k+" < next.key="+next.key);
				prev = next;
			} else {
				//System.out.println("skiplist.find: target="+k+" > next.key="+next.key);
				l--;
			}
		}
		
		result.prev = prev;
		result.node = prev.next(0);
		result.found = false;
		
		return result;
	}
	
	public FindResult<K,V> find(K target) {
		FindResult<K,V> result = findFirstGreaterOrEqual(target);
		if (!result.found)
			result.node = null;
		return result;
	}
	
	public V put(K k, V v) {
		Objects.requireNonNull(k);
		
		FindResult<K,V> result = find(k);
		if (result.node == null) {
			Node<K,V> node = new Node<K,V>(randomLevel(), k, v);
			result.prev.addNext(node);
			size++;
			if (node.next(0) == null)
				tail = node;
			return null;
		} else {
			V ret = result.node.value;
			result.node.value = v;
			return ret;
		}
	}
	
	public V put(Node<K,V> node) {
		Objects.requireNonNull(node);
		
		FindResult<K,V> result = find(node.key);
		if (result.node == null) {
			result.prev.addNext(node);
			size++;
			if (node.next(0) == null)
				tail = node;
			return null;
		} else {
			V ret = result.node.value;
			result.node.value = node.value;
			return ret;
		}
	}
	
	public V remove(K k) {
		FindResult<K,V> result = find(k);
		if (result.node != null) {
			result.node.remove();
			size--;
			if (result.node.next(0) == null) {
				tail = result.node.prev(0);
			}
		}
		
		return result.node == null ? null : result.node.value;
	}
	
	public class Iterator1 {
		
		Node<K,V> prev;
		Node<K,V> node;
		
		public Iterator1() {
			prev = null;
			node = head;
		}
		
		/**
		 *  Returns true iff the iterator is positioned at a valid node.
		 * @return
		 */
	    public boolean valid() {
	    	return (node != null && node != head);
	    }

		/**
		 *  Advances to the next position.</br>
		 *  REQUIRES: valid()
		 */
		public void next() {
			assert(valid());
			prev = node;
			node = node.next(0);
		}

		/**
		 *  Advances to the previous position.</br>
		 *  REQUIRES: valid()
		 */
		public void prev() {
			node = prev;
			prev = prev.prev(0);
		}

		/**
		 *  Advance to the first entry with a key >= target
		 * @param target
		 */
		public void seek(K target) {
			FindResult<K,V> result = findFirstGreaterOrEqual(target);
			prev = result.prev;
			node = result.node;
		}

		/**
		 *  Position at the first entry in list.
		 *  Final state of iterator is valid() iff list is not empty.
		 */
		public void seekToFirst() {
			prev = head;
			node = prev.next(0);
		}

		/**
		 *  Position at the last entry in list.
		 *  Final state of iterator is Valid() iff list is not empty.
		 */
		public void seekToLast() {
			node = tail;
			prev = node.prev(0);
		}
		
		public K key() {
			return node.key;
		}
		
		public V value() {
			return node.value;
		}
	}
	
	public Iterator1 iterator1() {
		return new Iterator1();
	}
	
	public class DefaultIterator implements Iterator<Map.Entry<K,V>> {
		Node<K,V> node;
		
		public DefaultIterator() {
			this.node = head;
		}
		
		@Override
		public boolean hasNext() {
			return node.next(0) != null;
		}
		
		@Override
		public Map.Entry<K, V> next() {
			node = node.next(0);
			return node;
		}
		
		@Override
		public void remove() {
			if (node == head || node == null)
				return;
			node.remove();
		}
	}
	
	public Map.Entry<K, V> firstEntry() {
		return head.next(0);
	}
	
	public Map.Entry<K, V> lastEntry() {
		return tail;
	}
	
	public Iterator<Map.Entry<K,V>> iterator() {
		return new DefaultIterator();
	}

	public int size() {
		return size;
	}
}
