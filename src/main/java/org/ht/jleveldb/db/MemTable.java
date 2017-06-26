package org.ht.jleveldb.db;

import java.util.Comparator;

import org.ht.jleveldb.Iterator0;
import org.ht.jleveldb.Status;
import org.ht.jleveldb.db.format.DBFormat;
import org.ht.jleveldb.db.format.InternalKeyComparator;
import org.ht.jleveldb.db.format.LookupKey;
import org.ht.jleveldb.db.format.ValueType;
import org.ht.jleveldb.util.ByteBuf;
import org.ht.jleveldb.util.ByteBufFactory;
import org.ht.jleveldb.util.Coding;
import org.ht.jleveldb.util.Object0;
import org.ht.jleveldb.util.Slice;

public class MemTable {
	
	static class KeyValueSlice {
		public byte[] data;
		public int keyOffset;
		public int internalKeySize;
		public int valueOffset;
		public int valueSize;
		
		public KeyValueSlice(byte[] data, int keyOffset, int internalKeySize, int valueOffset, int valueSize) {
			this.data = data;
			this.keyOffset = keyOffset;
			this.internalKeySize = internalKeySize;
			this.valueOffset = valueOffset;
			this.valueSize = valueSize;
		}
		
		final public Slice internalKeySlice() {
			return new Slice(data, keyOffset, internalKeySize);
		}
		
		final public Slice value() {
			return new Slice(data, valueOffset, valueSize);
		}
	}
	
	static class TableKeyComparator implements Comparator<Slice>{
		InternalKeyComparator comparator;
		
		public TableKeyComparator(InternalKeyComparator comparator) {
			this.comparator = comparator;
		}
		
	    public int compare(Slice a, Slice b) {
	    	// Internal keys are encoded as length-prefixed strings.
	    	// Slice a = getLengthPrefixedSlice(adata, 0);
	    	// Slice b = getLengthPrefixedSlice(bdata, 0);
	    	return comparator.compare(a, b);
	    }
	}
	
	static class MemTableIterator extends Iterator0 {
		
		SkipListMap<Slice, KeyValueSlice>.Iterator1 iter;
		
		public MemTableIterator(SkipListMap<Slice, KeyValueSlice> table) {
			iter = table.iterator1();
		}
		
		public void delete() {
			iter = null;
		}
		
		public boolean valid() {
			return iter.valid();
		}
		
		public void seekToFirst() {
			iter.seekToFirst();
		}
		
		public void seekToLast() {
			iter.seekToLast();
		}
		
		public void seek(Slice target0) {
			iter.seek(target0);
		}
		
		public void next() {
			iter.next();
		}
		
		public void prev() {
			iter.prev();
		}
		
		public Slice key() {
			return iter.key();
		}
		
		public Slice value() {
			return iter.value().value();
		}
		
		public Status status() {
			return Status.ok0();
		}
	}
	
	TableKeyComparator comparator;
	int refs;
	SkipListMap<Slice,KeyValueSlice> table;
	long approximateMemory = 0;
		
	public MemTable(InternalKeyComparator c) {
		comparator = new TableKeyComparator(c);
		refs = 0;
		table = new SkipListMap<Slice,KeyValueSlice>(12, 4, comparator);
	}
	
	public void delete() {
		//TODO: //assert(refs == 0);
		table = null;
	}
	
	/**
	 *  Increase reference count.
	 */
	public void ref() { 
		++refs; 
	}

	/** 
	 * Drop reference count.  Delete if no more references exist.
	 */
	public void unref() {
		--refs;
		//TODO: //assert(refs >= 0);
	    if (refs <= 0) {
	    	delete();
	    }
	}

	/**
	 * Returns an estimate of the number of bytes of data in use by this data structure. </br>
	 * It is safe to call when MemTable is being modified.</br>
	 */
	public long approximateMemoryUsage() {
		return approximateMemory;
	}
	
	public int entrySize() {
		return table.size();
	}
	
	/**
	 * Return an iterator that yields the contents of the memtable.</br></br>
	 * 
	 * The caller must ensure that the underlying MemTable remains live
	 * while the returned iterator is live.  The keys returned by this
	 * iterator are internal keys encoded by AppendInternalKey in the
	 * db/format.{h,cc} module.</br></br>
	 */
	public Iterator0 newIterator() {
		return new MemTableIterator(table);
	}
	
	/**
	 * Add an entry into memtable that maps key to value at the
	 * specified sequence number and with the specified type.</br>
	 * Typically value will be empty if type==kTypeDeletion.</br></br>
	 * 
	 * @param seq
	 * @param type ValueType
	 * @param key Slice
	 * @param value Slice
	 */
	public void add(long seq, ValueType type, Slice key, Slice value) {
		// Format:
		//   output : {internalKeySize:varint32, key:byte[internalKeySize], valSize:varint32, value:byte[valSize]}
		//   internalKey : {data:byte[size], (seq&type):uint64}
		
		int keyOffset = -1;
		int userKeySize = key.size();
		int valueOffset = -1;
		int valueSize = value.size();
		int internalKeySize = userKeySize + 8;
		int encodedLen = Coding.varNatLength(internalKeySize) + internalKeySize + Coding.varNatLength(valueSize) + valueSize;
		
		ByteBuf buf = ByteBufFactory.defaultByteBuf();
		buf.init(new byte[encodedLen], encodedLen);
		buf.writeVarNat32(internalKeySize);
		keyOffset = buf.position();
		buf.append(key.data, key.offset, key.size());
		buf.writeFixedNat64((seq << 8) | (type.type() & 0xFFL));
		buf.writeVarNat32(valueSize);
		valueOffset = buf.position();
		buf.append(value.data, value.offset, value.size());
		assert(buf.size() == encodedLen);
		
		approximateMemory += buf.capacity();
		
		KeyValueSlice kvs = new KeyValueSlice(buf.data(), keyOffset, internalKeySize, valueOffset, valueSize);
		//ParsedInternalKeySlice keySlice = new ParsedInternalKeySlice(seq, type, buf.data(), keyOffset, keySize);
		Slice keySlice = new Slice(buf.data(), keyOffset, internalKeySize);
		table.put(keySlice, kvs);
	}
	
	/**
	 * If memtable contains a value for key, store it in *value and return true.</br>
	 * If memtable contains a deletion for key, store a NotFound() error in *status and return true.</br>
	 * Else, return false.</br></br>
	 * 
	 * @param key
	 * @param value
	 * @param s
	 * @return
	 */
	public boolean get(LookupKey key, ByteBuf value, Object0<Status> s) {
		if (value != null)
			value.clear();
		Slice memkey = key.internalKey();
		SkipListMap<Slice,KeyValueSlice>.Iterator1 iter = table.iterator1();
		iter.seek(memkey);
		if (iter.valid()) {
			Slice ikey = iter.key();
		    //System.out.println("seek result: "+ikey);
		    if (comparator.comparator.userComparator().compare(
		    		new Slice(ikey.data, ikey.offset, ikey.size()-8), 
		    		key.userKey()) == 0) {
		    	// Correct user key
		    	ValueType vtype = DBFormat.extractValueType(ikey);
		    	if (vtype != null) {
			    	switch (vtype) {
			        case Value: {
			        	Slice v = iter.value().value();
			        	value.assign(v.data(), v.offset, v.size());
			        	return true;
			        }
			        case Deletion:
			        	s.setValue(Status.notFound(null));
			        	return true;
			    	}
		    	}
		    }
		}
		return false;
	}
	
	
	/**
	 * Encode a suitable internal key target for "target" and return it.</br>
	 * Uses scratch as scratch space.</br>
	 * 
	 * @param scratch [OUTPUT]
	 * @param target
	 */
	static void EncodeKey(ByteBuf scratch, Slice target) {
		scratch.clear();
		scratch.writeVarNat32(target.size());
		scratch.append(target.data(), target.size());
	}
	
	
	static Slice getLengthPrefixedSlice(byte[] data, int offset) {
		Slice tmp = new Slice(data, offset, 5);
		int len = Coding.getVarNat32Ptr(tmp);  // +5: we assume "p" is not corrupted
		offset = tmp.offset;
		tmp.init(data, offset, len);
		return tmp;
	}
}
