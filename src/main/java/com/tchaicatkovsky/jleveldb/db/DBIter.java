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
package com.tchaicatkovsky.jleveldb.db;

import java.util.Random;

import com.tchaicatkovsky.jleveldb.Iterator0;
import com.tchaicatkovsky.jleveldb.Status;
import com.tchaicatkovsky.jleveldb.db.format.DBFormat;
import com.tchaicatkovsky.jleveldb.db.format.ParsedInternalKey;
import com.tchaicatkovsky.jleveldb.db.format.ValueType;
import com.tchaicatkovsky.jleveldb.util.ByteBuf;
import com.tchaicatkovsky.jleveldb.util.ByteBufFactory;
import com.tchaicatkovsky.jleveldb.util.Comparator0;
import com.tchaicatkovsky.jleveldb.util.Slice;
import com.tchaicatkovsky.jleveldb.util.SliceFactory;

public class DBIter extends Iterator0 {

	// Which direction is the iterator currently moving?
	// (1) When moving forward, the internal iterator is positioned at
	//     the exact entry that yields this.key(), this.value()
	// (2) When moving backwards, the internal iterator is positioned
	//     just before all entries whose user key == this.key().
	enum Direction {
		kForward,
	    kReverse
	};
	
	DBImpl db;
	Comparator0 userComparator;
	Iterator0 iter;
	final long sequence;

	Status status = Status.ok0();
	ByteBuf savedKey = ByteBufFactory.newUnpooled();      // == current key when direction_== kReverse
	ByteBuf savedValue = ByteBufFactory.newUnpooled();    // == current raw value when direction == kReverse
	Direction direction;
	boolean valid;

	Random rnd;
	long bytesCounter;
	
	public DBIter(DBImpl db, Comparator0 cmp, Iterator0 iter, long seq,  int seed) {
		this.db = db;
		this.userComparator = cmp;
		this.iter = iter;
		this.sequence = seq;
		
		direction = Direction.kForward;
		valid = false;
		rnd = new Random(); //TODO change to Random0
		rnd.setSeed(seed);
		
		bytesCounter = randomPeriod();
	}
	
	@Override
	public void delete() {
		super.delete();
		db = null;
		savedKey = null;
		savedValue = null;
		if (iter != null) {
			iter.delete();
			iter = null;
		}
	}
	
	/**
	 *  Pick next gap with average value of DBFormat.kReadBytesPeriod.
	 * @return
	 */
	long randomPeriod() {
	    //return rnd_.Uniform(2*config::kReadBytesPeriod);
		return (long)(rnd.nextDouble() * 2 * DBFormat.kReadBytesPeriod);
	}
	
	boolean parseKey(ParsedInternalKey ikey) {
		Slice k = iter.key();
		long n = k.size() + iter.value().size();
		bytesCounter -= n;
		while (bytesCounter < 0) {
			bytesCounter += randomPeriod();
		    db.recordReadSample(k);
		}
		if (!ikey.parse(k)) {
		    status = Status.corruption("corrupted internal key in DBIter");
		    return false;
		} else {
		    return true;
		}
	}
	
	void saveKey(Slice k, ByteBuf dst) {
	    dst.assign(k.data(), k.offset(), k.size());
	}
	
	void findNextUserEntry(boolean skipping, ByteBuf skip) {
		// Loop until we hit an acceptable entry to yield		
		assert(iter.valid());
		assert(direction == Direction.kForward);
		ParsedInternalKey ikey = new ParsedInternalKey();
		do {
		    if (parseKey(ikey) && ikey.sequence <= sequence) {
		    	switch (ikey.type) {
		        case Deletion:
		        	// Arrange to skip all upcoming entries for this key since
		        	// they are hidden by this deletion.
		        	saveKey(ikey.userKey, skip);
		        	skipping = true;
		        	break;
		        case Value:
		        	if (skipping && userComparator.compare(ikey.userKey, skip) <= 0) {
		        		// Entry hidden
		        	} else {
		        		valid = true;
		        		savedKey.clear();
		        		return;
		        	}
		        	break;
		    	}
		    }
		    iter.next();
		} while (iter.valid());
		savedKey.clear();
		valid = false;
	}
	
	void findPrevUserEntry() {
		assert(direction == Direction.kReverse);

		ValueType valueType = ValueType.Deletion;
		if (iter.valid()) {
			do {
				ParsedInternalKey ikey = new ParsedInternalKey();
				if (parseKey(ikey) && ikey.sequence <= sequence) {
					if ((valueType != ValueType.Deletion) &&
							userComparator.compare(ikey.userKey, savedKey) < 0) {
						// We encountered a non-deleted value in entries for previous keys,
						break;
					}
					valueType = ikey.type;
					if (valueType == ValueType.Deletion) {
						savedKey.clear();
						clearSavedValue();
					} else {
						Slice rawValue = iter.value();
						if (savedValue.capacity() > rawValue.size() + 1048576) {
							ByteBuf empty = ByteBufFactory.newUnpooled();
							empty.swap(savedValue); //TODO: swap may affect the pooled ByteBuf
						}
						saveKey(DBFormat.extractUserKey(iter.key()), savedKey);
						savedValue.assign(rawValue.data(), rawValue.offset(), rawValue.size());
					}
				}
				iter.prev();
		    } while (iter.valid());
		}

		if (valueType == ValueType.Deletion) {
			// End
		    valid = false;
		    savedKey.clear();
		    clearSavedValue();
		    direction = Direction.kForward;
		} else {
		    valid = true;
		}
	}

	final void clearSavedValue() {
	    if (savedValue.capacity() > 1048576) {
	    	ByteBuf empty = ByteBufFactory.newUnpooled();
	    	empty.swap(savedValue); //TODO: swap may affect the pooled ByteBuf
	    } else {
	    	savedValue.clear();
	    }
	}
	
	@Override
	final public boolean valid() {
		return valid;
	}

	@Override
	public void seekToFirst() {
		direction = Direction.kForward;
		clearSavedValue();
		iter.seekToFirst();
		if (iter.valid()) {
		    findNextUserEntry(false, savedKey);
		} else {
		    valid = false;
		}
	}

	@Override
	public void seekToLast() {
		direction = Direction.kReverse;
		clearSavedValue();
		iter.seekToLast();
		findPrevUserEntry();
	}

	@Override
	public void seek(Slice target) {
		direction = Direction.kForward;
		clearSavedValue();
		savedKey.clear();
		DBFormat.appendInternalKey(savedKey, new ParsedInternalKey(target, sequence, DBFormat.kValueTypeForSeek));
		iter.seek(SliceFactory.newUnpooled(savedKey));
		if (iter.valid()) {
			findNextUserEntry(false, savedKey);
		} else {
			valid = false;
		}
	}

	@Override
	public void next() {
		assert(valid);

		if (direction == Direction.kReverse) {
		    direction = Direction.kForward;
		    // iter is pointing just before the entries for this.key(),
		    // so advance into the range of entries for this.key() and then
		    // use the normal skipping code below.
		    if (!iter.valid()) {
		    	iter.seekToFirst();
		    } else {
		    	iter.next();
		    }
		    if (!iter.valid()) {
		    	valid = false;
		    	savedKey.clear();
		    	return;
		    }
		    // savedKey already contains the key to skip past.
		} else {
		    // Store in savedKey the current key so we skip it below.
		    saveKey(DBFormat.extractUserKey(iter.key()), savedKey);
		}

		findNextUserEntry(true, savedKey);
	}

	@Override
	public void prev() {
		assert(valid);

		if (direction == Direction.kForward) {
		    // iter is pointing at the current entry.  Scan backwards until
		    // the key changes so we can use the normal reverse scanning code.
		    assert(iter.valid());  // Otherwise valid would have been false
		    saveKey(DBFormat.extractUserKey(iter.key()), savedKey);
		    while (true) {
		    	iter.prev();
		    	if (!iter.valid()) {
		    		valid = false;
		    		savedKey.clear();
		    		clearSavedValue();
		    		return;
		    	}
		    	if (userComparator.compare(DBFormat.extractUserKey(iter.key()), savedKey) < 0) {
		    		break;
		    	}
		    }
		    direction = Direction.kReverse;
		}
		
		findPrevUserEntry();
	}

	/**
	 * @return user key
	 */
	@Override
	public Slice key() {
		assert(valid);
		return (direction == Direction.kForward) ? 
				DBFormat.extractUserKey(iter.key()) : SliceFactory.newUnpooled(savedKey);
	}

	@Override
	public Slice value() {
		assert(valid);
		return (direction == Direction.kForward) ? 
				iter.value() : SliceFactory.newUnpooled(savedValue);
	}

	@Override
	public Status status() {
		if (status.ok()) {
			return iter.status();
		} else {
			return status;
		}
	}

	/**
	 * Return a new iterator that converts internal keys (yielded by
	 * "internalIter") that were live at the specified "sequence" number
	 * into appropriate user keys.
	 * 
	 * @param db
	 * @param userKeyComparator
	 * @param internalIter
	 * @param sequence
	 * @param seed
	 * @return
	 */
	public static Iterator0 newDBIterator(
						DBImpl db,
						Comparator0 userKeyComparator,
						Iterator0 internalIter,
						long sequence,
						int seed) {
		return new DBIter(db, userKeyComparator, internalIter, sequence, seed);
	} 
}
