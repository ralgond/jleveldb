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
package com.tchaicatkovsky.jleveldb.table;

import java.util.List;

import com.tchaicatkovsky.jleveldb.Iterator0;
import com.tchaicatkovsky.jleveldb.Status;
import com.tchaicatkovsky.jleveldb.util.Comparator0;
import com.tchaicatkovsky.jleveldb.util.Slice;

/**
 * Return an iterator that provided the union of the data in
 * children[0,n-1].  Takes ownership of the child iterators and
 * will delete them when the result iterator is deleted.</br></br>
 *
 * The result does no duplicate suppression.  I.e., if a particular
 * key is present in K child iterators, it will be yielded K times.</br></br>
 *
 * <b>REQUIRES: n >= 0</b>
 */
public class Merger {
	static class MergingIterator extends Iterator0 {
		// We might want to use a heap in case there are lots of children.
		// For now we use a simple array since we expect a very small number
		// of children in leveldb.
		Comparator0 comparator;
		Iterator0Wrapper[] children;
		int n;
		Iterator0Wrapper current;

		// Which direction is the iterator moving?
		enum Direction {
		    kForward,
		    kReverse
		};
		Direction direction;
		  
		public MergingIterator(Comparator0 comparator, List<Iterator0> children0) {
			this.comparator = comparator;
			n = children0.size();
	        children = new Iterator0Wrapper[n];
	        current = null;
	        direction = Direction.kForward;
	        for (int i = 0; i < n; i++) {
	        	children[i] = new Iterator0Wrapper();
	        	children[i].set(children0.get(i));
	        }
	    }
		
		@Override
		public void delete() {
			super.delete();
			for (int i = 0; i < n; i++)
				children[i].delete();
		}
		
		public boolean valid() {
			return (current != null);
		}
		
		public void seekToFirst() {
		    for (int i = 0; i < n; i++)
		        children[i].seekToFirst();

		    findSmallest();
		    direction = Direction.kForward;
		}
		
		public void seekToLast() {
		    for (int i = 0; i < n; i++)
		        children[i].seekToLast();

		    findLargest();
		    direction = Direction.kReverse;
		}
		
		public void seek(Slice target) {
		    for (int i = 0; i < n; i++)
		        children[i].seek(target);

		    findSmallest();
		    direction = Direction.kForward;
		}
		
		public void next() {
			assert(valid());

		    // Ensure that all children are positioned after key().
		    // If we are moving in the forward direction, it is already
		    // true for all of the non-current_ children since current_ is
		    // the smallest child and key() == current_->key().  Otherwise,
		    // we explicitly position the non-current_ children.
			if (direction != Direction.kForward) {
				for (int i = 0; i < n; i++) {
			        Iterator0Wrapper child = children[i];
			        if (child != current) {
			        	child.seek(key());
			        	if (child.valid() && comparator.compare(key(), child.key()) == 0)
			        		child.next();
			        }
			    }
			    direction = Direction.kForward;
			}
			
			//Logger0.debug("MergingIterator.next\n");

			current.next();
			findSmallest();
		}
		
		public void prev() {
		    assert(valid());

		    // Ensure that all children are positioned before key().
		    // If we are moving in the reverse direction, it is already
		    // true for all of the non-current_ children since current_ is
		    // the largest child and key() == current_->key().  Otherwise,
		    // we explicitly position the non-current_ children.
		    if (direction != Direction.kReverse) {
		    	for (int i = 0; i < n; i++) {
		    		Iterator0Wrapper child = children[i];
		    		if (child != current) {
		    			child.seek(key());
		    			if (child.valid()) {
		    				// Child is at first entry >= key().  Step back one to be < key()
		    				child.prev();
		    			} else {
		    				// Child has no entries >= key().  Position at last entry.
		    				child.seekToLast();
		    			}
		    		}
		    	}
		    	direction = Direction.kReverse;
		    }

		    current.prev();
		    findLargest();
		}
		
		public Slice key() {
			assert(valid());
			return current.key();
		}
		
		public Slice value() {
			assert(valid());
		    return current.value();
		}
		
		public Status status() {
		    Status status = Status.ok0();
		    for (int i = 0; i < n; i++) {
		      status = children[i].status();
		      if (!status.ok()) {
		    	  break;
		      }
		    }
		    return status;
		}
		
		
		public void findSmallest() {
			Iterator0Wrapper smallest = null;
			for (int i = 0; i < n; i++) {
			    Iterator0Wrapper child = children[i];
			    
			    if (child.valid()) {
			    	if (smallest == null) {
			    		smallest = child;
			    	} else if (comparator.compare(child.key(), smallest.key()) < 0) {
			    		smallest = child;
			    	}
			    }
			}
			current = smallest;
		}
		
		public void findLargest() {
			Iterator0Wrapper largest = null;
			for (int i = n-1; i >= 0; i--) {
			    Iterator0Wrapper child = children[i];
			    if (child.valid()) {
			    	if (largest == null) {
			    		largest = child;
			    	} else if (comparator.compare(child.key(), largest.key()) > 0) {
			    		largest = child;
			    	}
			    }
			}
			current = largest;
		}
	}
	
	public static Iterator0 newMergingIterator(Comparator0 comparator, List<Iterator0> children) {
		int n = children.size();
		assert(n >= 0);
		if (n == 0) {
		    return Iterator0.newEmptyIterator();
		} else if (n == 1) {
		    return children.get(0);
		} else {
		    return new MergingIterator(comparator, children);
		}
	}
}
