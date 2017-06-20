package org.ht.jleveldb.db;

import org.ht.jleveldb.Status;
import org.ht.jleveldb.WriteBatch;
import org.ht.jleveldb.db.format.ValueType;
import org.ht.jleveldb.util.Coding;
import org.ht.jleveldb.util.Slice;

//WriteBatchInternal provides static methods for manipulating a
//WriteBatch that we don't want in the public WriteBatch interface.
public class WriteBatchInternal {
	
	// WriteBatch header has an 8-byte sequence number followed by a 4-byte count.
	public static int HEADER = 12;
	
    // Return the number of entries in the batch.
    public static int count(WriteBatch b) {
    	return Coding.decodeFixedNat32(b.rep.data(), 8);
    }

    // Set the count for the number of entries in the batch.
    public static void setCount(WriteBatch b, int n) {
    	Coding.encodeFixedNat32(b.rep.data(), 8, 8 + 4, n);
    }

    // Return the sequence number for the start of this batch.
    public static long sequence(WriteBatch b) {
    	return  Coding.decodeFixedNat64(b.rep.data(), 0);
    }

    // Store the specified number as the sequence number for the start of
    // this batch.
    public static void setSequence(WriteBatch b, long seq) {
    	Coding.encodeFixedNat64(b.rep.data(), 0, 8, seq);
    }

    public static Slice contents(WriteBatch batch) {
        return new Slice(batch.rep);
    }

    public static long byteSize(WriteBatch batch) {
        return batch.rep.size();
    }
    
    static class MemTableInserter implements WriteBatch.Handler {
    	long sequence;
    	MemTable memtable;

    	public void put(Slice key, Slice value) {
    		memtable.add(sequence, ValueType.Value, key, value);
    	    sequence++;
    	}
    	public void delete(Slice key) {
    		memtable.add(sequence, ValueType.Deletion, key, new Slice());
    		sequence++;
    	}
    };

    public static Status insertInto(WriteBatch b, MemTable memtable) {
    	MemTableInserter inserter = new MemTableInserter();
    	inserter.sequence = WriteBatchInternal.sequence(b);
    	inserter.memtable = memtable;
    	return b.iterate(inserter);
    }
    
    public static void setContents(WriteBatch b, Slice contents) {
    	assert(contents.size() >= HEADER);
    	b.rep.assign(contents.data(), contents.offset(), contents.size());
    }

    public static void append(WriteBatch dst, WriteBatch src) {
    	setCount(dst, count(dst) + count(src));
    	assert(src.rep.size() >= HEADER);
    	dst.rep.append(src.rep.data(), HEADER, src.rep.size() - HEADER);
    }
}
